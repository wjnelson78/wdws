#!/usr/bin/env python3
"""
Patch dashboard app.py to add MCP Accounts + Query Analytics tabs.

Adds:
1. API endpoints: /api/mcp/clients, /api/mcp/query-log, /api/mcp/analytics, /api/mcp/clients/{id}/toggle
2. Nav buttons for "MCP Accounts" and "Query Analytics"
3. HTML tab sections
4. JavaScript functions for loading data
"""

import sys

def patch(source: str) -> str:
    # ═══════════════════════════════════════════════════════════
    # 1. ADD API ENDPOINTS (before the routes list)
    # ═══════════════════════════════════════════════════════════
    api_code = '''

# ── MCP Accounts & Query Analytics ───────────────────────────

async def api_mcp_clients(request):
    """List and manage OAuth clients."""
    err = await _check_auth(request)
    if err:
        return err
    p = await get_pool()
    rows = await p.fetch("""
        SELECT c.client_id, c.client_name, c.is_active, c.created_at, c.last_used,
               c.redirect_uris, c.scopes, c.description,
               COALESCE(c.total_tool_calls, 0) as total_tool_calls,
               (SELECT COUNT(*) FROM ops.oauth_access_tokens t WHERE t.client_id = c.client_id) as active_tokens,
               (SELECT COUNT(*) FROM ops.mcp_query_log q WHERE q.client_id = c.client_id) as logged_calls,
               (SELECT MAX(q.created_at) FROM ops.mcp_query_log q WHERE q.client_id = c.client_id) as last_query
        FROM ops.oauth_clients c
        ORDER BY c.created_at
    """)
    clients = []
    for r in rows:
        clients.append({
            "client_id": r["client_id"],
            "client_name": r["client_name"],
            "is_active": r["is_active"],
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            "last_used": r["last_used"].isoformat() if r["last_used"] else None,
            "redirect_uris": list(r["redirect_uris"] or []),
            "scopes": list(r["scopes"] or []),
            "description": r["description"] or "",
            "total_tool_calls": r["total_tool_calls"],
            "active_tokens": r["active_tokens"],
            "logged_calls": r["logged_calls"],
            "last_query": r["last_query"].isoformat() if r["last_query"] else None,
        })
    return JSONResponse({"clients": clients})


async def api_mcp_client_toggle(request):
    """Toggle a client active/inactive."""
    err = await _check_auth(request)
    if err:
        return err
    cid = request.path_params["cid"]
    p = await get_pool()
    row = await p.fetchrow("SELECT is_active FROM ops.oauth_clients WHERE client_id = $1", cid)
    if not row:
        return JSONResponse({"error": "Client not found"}, status_code=404)
    new_state = not row["is_active"]
    await p.execute("UPDATE ops.oauth_clients SET is_active = $1 WHERE client_id = $2", new_state, cid)
    if not new_state:
        # Revoke all tokens when disabling
        await p.execute("DELETE FROM ops.oauth_access_tokens WHERE client_id = $1", cid)
        await p.execute("DELETE FROM ops.oauth_refresh_tokens WHERE client_id = $1", cid)
    return JSONResponse({"client_id": cid, "is_active": new_state})


async def api_mcp_query_log(request):
    """Paginated query log with filters."""
    err = await _check_auth(request)
    if err:
        return err
    params = request.query_params
    client_id = params.get("client_id", "")
    tool_name = params.get("tool_name", "")
    limit = min(int(params.get("limit", "50")), 200)
    offset = int(params.get("offset", "0"))
    hours = int(params.get("hours", "168"))  # default 7 days

    conditions = ["created_at > now() - interval '1 hour' * $1"]
    args = [hours]
    idx = 2

    if client_id:
        conditions.append(f"client_id = ${idx}")
        args.append(client_id)
        idx += 1
    if tool_name:
        conditions.append(f"tool_name = ${idx}")
        args.append(tool_name)
        idx += 1

    where = " AND ".join(conditions)

    p = await get_pool()
    total = await p.fetchval(f"SELECT COUNT(*) FROM ops.mcp_query_log WHERE {where}", *args)

    rows = await p.fetch(f"""
        SELECT id, client_id, client_name, tool_name, arguments, result_summary,
               result_count, duration_ms, error, created_at
        FROM ops.mcp_query_log
        WHERE {where}
        ORDER BY created_at DESC
        LIMIT ${idx} OFFSET ${idx + 1}
    """, *args, limit, offset)

    entries = []
    for r in rows:
        entries.append({
            "id": r["id"],
            "client_id": r["client_id"],
            "client_name": r["client_name"],
            "tool_name": r["tool_name"],
            "arguments": r["arguments"] if r["arguments"] else {},
            "result_summary": r["result_summary"] if r["result_summary"] else {},
            "result_count": r["result_count"],
            "duration_ms": r["duration_ms"],
            "error": r["error"],
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
        })

    return JSONResponse({"entries": entries, "total": total, "limit": limit, "offset": offset})


async def api_mcp_analytics(request):
    """Aggregated analytics for MCP tool usage."""
    err = await _check_auth(request)
    if err:
        return err
    hours = int(request.query_params.get("hours", "168"))
    p = await get_pool()

    # Tool usage breakdown
    tool_stats = await p.fetch("""
        SELECT tool_name,
               COUNT(*) as call_count,
               AVG(duration_ms)::int as avg_ms,
               MAX(duration_ms) as max_ms,
               SUM(result_count) as total_results,
               COUNT(CASE WHEN error IS NOT NULL THEN 1 END) as error_count
        FROM ops.mcp_query_log
        WHERE created_at > now() - interval '1 hour' * $1
        GROUP BY tool_name
        ORDER BY call_count DESC
    """, hours)

    # Client usage breakdown
    client_stats = await p.fetch("""
        SELECT client_id, client_name,
               COUNT(*) as call_count,
               AVG(duration_ms)::int as avg_ms,
               COUNT(DISTINCT tool_name) as tools_used,
               COUNT(CASE WHEN error IS NOT NULL THEN 1 END) as error_count,
               MAX(created_at) as last_call
        FROM ops.mcp_query_log
        WHERE created_at > now() - interval '1 hour' * $1
        GROUP BY client_id, client_name
        ORDER BY call_count DESC
    """, hours)

    # Hourly activity (last 24h)
    hourly = await p.fetch("""
        SELECT date_trunc('hour', created_at) as hour,
               COUNT(*) as calls,
               COUNT(DISTINCT client_id) as clients
        FROM ops.mcp_query_log
        WHERE created_at > now() - interval '24 hours'
        GROUP BY date_trunc('hour', created_at)
        ORDER BY hour
    """)

    # Recent errors
    errors = await p.fetch("""
        SELECT id, client_id, client_name, tool_name, arguments, error, created_at
        FROM ops.mcp_query_log
        WHERE error IS NOT NULL AND created_at > now() - interval '1 hour' * $1
        ORDER BY created_at DESC
        LIMIT 20
    """, hours)

    # Overall totals
    totals = await p.fetchrow("""
        SELECT COUNT(*) as total_calls,
               COUNT(DISTINCT client_id) as unique_clients,
               COUNT(DISTINCT tool_name) as unique_tools,
               AVG(duration_ms)::int as avg_latency,
               COUNT(CASE WHEN error IS NOT NULL THEN 1 END) as total_errors
        FROM ops.mcp_query_log
        WHERE created_at > now() - interval '1 hour' * $1
    """, hours)

    return JSONResponse({
        "period_hours": hours,
        "totals": dict(totals) if totals else {},
        "by_tool": [dict(r) for r in tool_stats],
        "by_client": [{**dict(r), "last_call": r["last_call"].isoformat() if r["last_call"] else None} for r in client_stats],
        "hourly": [{"hour": r["hour"].isoformat(), "calls": r["calls"], "clients": r["clients"]} for r in hourly],
        "recent_errors": [{
            "id": r["id"], "client_id": r["client_id"], "client_name": r["client_name"],
            "tool_name": r["tool_name"], "arguments": r["arguments"] if r["arguments"] else {},
            "error": r["error"], "created_at": r["created_at"].isoformat() if r["created_at"] else None,
        } for r in errors],
    })

'''
    # Insert before the routes list
    marker = "# ══════════════════════════════════════════════════════════════\n#  ROUTES + MIDDLEWARE"
    if "api_mcp_clients" not in source:
        source = source.replace(marker, api_code + "\n" + marker)

    # ═══════════════════════════════════════════════════════════
    # 2. ADD ROUTES
    # ═══════════════════════════════════════════════════════════
    route_marker = '    # Chat\n'
    new_routes = '''    # MCP Accounts & Analytics
    Route("/api/mcp/clients", api_mcp_clients),
    Route("/api/mcp/clients/{cid}/toggle", api_mcp_client_toggle, methods=["POST"]),
    Route("/api/mcp/query-log", api_mcp_query_log),
    Route("/api/mcp/analytics", api_mcp_analytics),
'''
    if "/api/mcp/clients" not in source:
        source = source.replace(route_marker, new_routes + route_marker)

    # ═══════════════════════════════════════════════════════════
    # 3. ADD NAV BUTTONS
    # ═══════════════════════════════════════════════════════════
    chat_nav = """  <button data-tab="chat" onclick="switchTab('chat')">💬 AI Chat</button>"""
    new_nav = """  <button data-tab="chat" onclick="switchTab('chat')">💬 AI Chat</button>
  <button data-tab="mcp" onclick="switchTab('mcp')">🔑 MCP Accounts</button>
  <button data-tab="analytics" onclick="switchTab('analytics')">📈 Query Analytics</button>"""
    if "data-tab=\"mcp\"" not in source:
        source = source.replace(chat_nav, new_nav)

    # ═══════════════════════════════════════════════════════════
    # 4. ADD TAB SECTIONS (before </main>)
    # ═══════════════════════════════════════════════════════════
    tab_html = '''
<!-- ═══ MCP ACCOUNTS TAB ═══ -->
<section id="tab-mcp" class="hidden">
  <div class="section-title">🔑 OAuth Client Accounts</div>
  <p style="color:var(--muted);font-size:.85rem;margin-bottom:16px">
    Manage AI clients (ChatGPT, Claude Desktop) connected via OAuth 2.0 to the MCP server at
    <code style="color:var(--cyan)">klunky.12432.net</code>
  </p>
  <div id="mcp-clients-grid"><div class="empty-state"><span class="spinner"></span></div></div>

  <div class="section-title" style="margin-top:24px">🔒 Active Access Tokens</div>
  <div id="mcp-tokens-info" style="color:var(--muted);font-size:.85rem">Loading…</div>
</section>

<!-- ═══ QUERY ANALYTICS TAB ═══ -->
<section id="tab-analytics" class="hidden">
  <div style="display:flex;gap:12px;align-items:center;margin-bottom:16px;flex-wrap:wrap">
    <div class="section-title" style="margin:0">📈 MCP Query Analytics</div>
    <select id="analytics-period" onchange="loadAnalytics()" style="padding:6px 10px;border-radius:var(--radius);border:1px solid var(--border);background:var(--surface);color:var(--text);font-size:.85rem">
      <option value="24">Last 24 hours</option>
      <option value="168" selected>Last 7 days</option>
      <option value="720">Last 30 days</option>
      <option value="2160">Last 90 days</option>
    </select>
    <button class="btn btn-outline btn-sm" onclick="loadAnalytics()">Refresh</button>
  </div>

  <div id="analytics-summary" class="cards"><div class="empty-state"><span class="spinner"></span></div></div>

  <div class="cols-2" style="margin-top:16px">
    <div>
      <div class="section-title">🛠️ Tool Usage Breakdown</div>
      <div id="analytics-tools"></div>
    </div>
    <div>
      <div class="section-title">👤 Client Usage Breakdown</div>
      <div id="analytics-clients"></div>
    </div>
  </div>

  <div class="section-title" style="margin-top:24px">⚠️ Recent Errors</div>
  <div id="analytics-errors"></div>

  <div class="section-title" style="margin-top:24px">📋 Query Log
    <span style="font-size:.75rem;color:var(--muted);font-weight:400">— Every tool call by AI clients</span>
  </div>
  <div style="display:flex;gap:8px;align-items:center;margin-bottom:12px;flex-wrap:wrap">
    <select id="qlog-client" onchange="qlogOffset=0;loadQueryLog()" style="padding:6px;border-radius:var(--radius);border:1px solid var(--border);background:var(--surface);color:var(--text);font-size:.85rem">
      <option value="">All Clients</option>
    </select>
    <select id="qlog-tool" onchange="qlogOffset=0;loadQueryLog()" style="padding:6px;border-radius:var(--radius);border:1px solid var(--border);background:var(--surface);color:var(--text);font-size:.85rem">
      <option value="">All Tools</option>
    </select>
    <span id="qlog-count" style="color:var(--muted);font-size:.8rem"></span>
    <div style="margin-left:auto;display:flex;gap:4px">
      <button class="btn btn-outline btn-sm" id="qlog-prev" onclick="qlogPrev()" disabled>&laquo; Prev</button>
      <button class="btn btn-outline btn-sm" id="qlog-next" onclick="qlogNext()">Next &raquo;</button>
    </div>
  </div>
  <div id="qlog-table"></div>
</section>

'''
    end_main = "\n</main>"
    if "tab-mcp" not in source:
        source = source.replace(end_main, tab_html + end_main)

    # ═══════════════════════════════════════════════════════════
    # 5. ADD switchTab ENTRIES
    # ═══════════════════════════════════════════════════════════
    old_loaders = """    email: loadEmail,
    chat: loadChat,"""
    new_loaders = """    email: loadEmail,
    chat: loadChat,
    mcp: loadMcpAccounts,
    analytics: loadAnalytics,"""
    if "mcp: loadMcpAccounts" not in source:
        source = source.replace(old_loaders, new_loaders)

    # ═══════════════════════════════════════════════════════════
    # 6. ADD JAVASCRIPT FUNCTIONS (before closing </script>)
    # ═══════════════════════════════════════════════════════════
    js_code = '''
// ═══════════════════════════════════════════════════════════
//  TAB: MCP ACCOUNTS
// ═══════════════════════════════════════════════════════════
async function loadMcpAccounts(){
  try {
    const d = await api('/api/mcp/clients');
    const clients = d.clients || [];
    let html = '<div class="cards">';
    for (const c of clients) {
      const statusBadge = c.is_active
        ? '<span style="color:var(--green);font-weight:600">● Active</span>'
        : '<span style="color:var(--red);font-weight:600">● Disabled</span>';
      const icon = c.client_id === 'chatgpt' ? '🤖' : c.client_id === 'claude-desktop' ? '🧠' : '🔌';
      html += `
        <div class="card" style="min-width:320px;flex:1">
          <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
            <h3 style="margin:0;font-size:1.1rem">${icon} ${esc(c.client_name || c.client_id)}</h3>
            ${statusBadge}
          </div>
          <div style="display:grid;grid-template-columns:auto 1fr;gap:4px 12px;font-size:.85rem">
            <span style="color:var(--muted)">Client ID:</span>
            <code style="color:var(--cyan);font-size:.8rem">${esc(c.client_id)}</code>
            <span style="color:var(--muted)">Scopes:</span>
            <span>${(c.scopes||[]).map(s=>'<code style="background:var(--surface);padding:1px 6px;border-radius:4px;font-size:.75rem">'+esc(s)+'</code>').join(' ')}</span>
            <span style="color:var(--muted)">Redirect URIs:</span>
            <span style="font-size:.8rem;word-break:break-all">${(c.redirect_uris||[]).map(u=>esc(u)).join('<br>')}</span>
            <span style="color:var(--muted)">Created:</span>
            <span>${fmtTime(c.created_at)}</span>
            <span style="color:var(--muted)">Last Used:</span>
            <span>${c.last_used ? fmtTime(c.last_used) : '<span style="color:var(--muted)">Never</span>'}</span>
            <span style="color:var(--muted)">Last Query:</span>
            <span>${c.last_query ? fmtTime(c.last_query) : '<span style="color:var(--muted)">None</span>'}</span>
            <span style="color:var(--muted)">Total Calls:</span>
            <span style="font-weight:600;color:var(--accent)">${fmtNum(c.logged_calls)}</span>
            <span style="color:var(--muted)">Active Tokens:</span>
            <span>${c.active_tokens}</span>
          </div>
          <div style="margin-top:12px;display:flex;gap:6px">
            <button class="btn btn-sm ${c.is_active ? 'btn-outline' : 'btn-primary'}"
              onclick="toggleMcpClient('${esc(c.client_id)}')">
              ${c.is_active ? '⏸ Disable' : '▶ Enable'}
            </button>
          </div>
        </div>`;
    }
    if (!clients.length) html += '<div class="empty-state">No OAuth clients registered</div>';
    html += '</div>';
    $('mcp-clients-grid').innerHTML = html;

    // Token info
    $('mcp-tokens-info').innerHTML = clients.map(c =>
      `<strong>${esc(c.client_name || c.client_id)}</strong>: ${c.active_tokens} active token(s)`
    ).join(' &nbsp;|&nbsp; ');
  } catch(e) {
    $('mcp-clients-grid').innerHTML = '<div class="empty-state">Error: '+esc(e.message)+'</div>';
  }
}

async function toggleMcpClient(cid){
  if(!confirm('Toggle this client?')) return;
  try {
    await api('/api/mcp/clients/'+cid+'/toggle', {method:'POST'});
    loadMcpAccounts();
  } catch(e) { alert('Error: '+e.message); }
}

// ═══════════════════════════════════════════════════════════
//  TAB: QUERY ANALYTICS
// ═══════════════════════════════════════════════════════════
let qlogOffset = 0;
const QLOG_LIMIT = 50;

async function loadAnalytics(){
  const hours = $('analytics-period').value;
  try {
    const d = await api('/api/mcp/analytics?hours=' + hours);
    const t = d.totals || {};

    // Summary cards
    $('analytics-summary').innerHTML = `
      <div class="card"><div class="card-label">Total Calls</div><div class="card-value">${fmtNum(t.total_calls)}</div></div>
      <div class="card"><div class="card-label">Unique Clients</div><div class="card-value">${fmtNum(t.unique_clients)}</div></div>
      <div class="card"><div class="card-label">Tools Used</div><div class="card-value">${fmtNum(t.unique_tools)}</div></div>
      <div class="card"><div class="card-label">Avg Latency</div><div class="card-value">${t.avg_latency||0}ms</div></div>
      <div class="card"><div class="card-label">Errors</div><div class="card-value" style="color:${t.total_errors?'var(--red)':'var(--green)'}">${fmtNum(t.total_errors)}</div></div>
    `;

    // Tool usage table
    const tools = d.by_tool || [];
    let toolHtml = '<table class="data-table"><thead><tr><th>Tool</th><th>Calls</th><th>Avg ms</th><th>Max ms</th><th>Results</th><th>Errors</th></tr></thead><tbody>';
    for (const t of tools) {
      toolHtml += `<tr>
        <td><code style="color:var(--cyan)">${esc(t.tool_name)}</code></td>
        <td style="font-weight:600">${fmtNum(t.call_count)}</td>
        <td>${t.avg_ms||0}ms</td>
        <td>${t.max_ms||0}ms</td>
        <td>${fmtNum(t.total_results)}</td>
        <td style="color:${t.error_count?'var(--red)':'var(--green)'}">${t.error_count}</td>
      </tr>`;
    }
    toolHtml += '</tbody></table>';
    if (!tools.length) toolHtml = '<div class="empty-state">No tool data yet</div>';
    $('analytics-tools').innerHTML = toolHtml;

    // Client usage table
    const clients = d.by_client || [];
    let clientHtml = '<table class="data-table"><thead><tr><th>Client</th><th>Calls</th><th>Tools</th><th>Avg ms</th><th>Errors</th><th>Last Call</th></tr></thead><tbody>';
    for (const c of clients) {
      const icon = c.client_id === 'chatgpt' ? '🤖' : c.client_id === 'claude-desktop' ? '🧠' : '🔌';
      clientHtml += `<tr>
        <td>${icon} ${esc(c.client_name||c.client_id)}</td>
        <td style="font-weight:600">${fmtNum(c.call_count)}</td>
        <td>${c.tools_used}</td>
        <td>${c.avg_ms||0}ms</td>
        <td style="color:${c.error_count?'var(--red)':'var(--green)'}">${c.error_count}</td>
        <td>${fmtTime(c.last_call)}</td>
      </tr>`;
    }
    clientHtml += '</tbody></table>';
    if (!clients.length) clientHtml = '<div class="empty-state">No client data yet</div>';
    $('analytics-clients').innerHTML = clientHtml;

    // Recent errors
    const errs = d.recent_errors || [];
    let errHtml = '';
    if (!errs.length) {
      errHtml = '<div style="color:var(--green);font-size:.85rem">✓ No errors in this period</div>';
    } else {
      errHtml = '<table class="data-table"><thead><tr><th>Time</th><th>Client</th><th>Tool</th><th>Error</th></tr></thead><tbody>';
      for (const e of errs) {
        errHtml += `<tr>
          <td>${fmtTime(e.created_at)}</td>
          <td>${esc(e.client_name||e.client_id)}</td>
          <td><code>${esc(e.tool_name)}</code></td>
          <td style="color:var(--red);font-size:.8rem;max-width:400px;overflow:hidden;text-overflow:ellipsis">${esc(e.error)}</td>
        </tr>`;
      }
      errHtml += '</tbody></table>';
    }
    $('analytics-errors').innerHTML = errHtml;

    // Populate filter dropdowns
    populateQlogFilters(d);

  } catch(e) {
    $('analytics-summary').innerHTML = '<div class="empty-state">Error: '+esc(e.message)+'</div>';
  }

  loadQueryLog();
}

function populateQlogFilters(analytics){
  const clientSel = $('qlog-client');
  const toolSel = $('qlog-tool');
  const curClient = clientSel.value;
  const curTool = toolSel.value;

  // Reset
  clientSel.innerHTML = '<option value="">All Clients</option>';
  toolSel.innerHTML = '<option value="">All Tools</option>';

  for (const c of (analytics.by_client||[])) {
    const opt = document.createElement('option');
    opt.value = c.client_id;
    opt.textContent = (c.client_name||c.client_id) + ' (' + c.call_count + ')';
    clientSel.appendChild(opt);
  }
  for (const t of (analytics.by_tool||[])) {
    const opt = document.createElement('option');
    opt.value = t.tool_name;
    opt.textContent = t.tool_name + ' (' + t.call_count + ')';
    toolSel.appendChild(opt);
  }
  clientSel.value = curClient;
  toolSel.value = curTool;
}

async function loadQueryLog(){
  const hours = $('analytics-period').value;
  const client = $('qlog-client').value;
  const tool = $('qlog-tool').value;
  let url = `/api/mcp/query-log?hours=${hours}&limit=${QLOG_LIMIT}&offset=${qlogOffset}`;
  if (client) url += '&client_id=' + encodeURIComponent(client);
  if (tool) url += '&tool_name=' + encodeURIComponent(tool);

  try {
    const d = await api(url);
    const entries = d.entries || [];
    const total = d.total || 0;

    $('qlog-count').textContent = `${qlogOffset+1}–${Math.min(qlogOffset+QLOG_LIMIT, total)} of ${fmtNum(total)}`;
    $('qlog-prev').disabled = qlogOffset === 0;
    $('qlog-next').disabled = qlogOffset + QLOG_LIMIT >= total;

    let html = '<table class="data-table"><thead><tr><th>Time</th><th>Client</th><th>Tool</th><th>Arguments</th><th>Results</th><th>Latency</th><th>Status</th></tr></thead><tbody>';
    for (const e of entries) {
      const args = e.arguments || {};
      const argStr = Object.entries(args).map(([k,v]) =>
        `<span style="color:var(--muted)">${esc(k)}=</span>${esc(String(v).substring(0,60))}`
      ).join(', ');

      const summary = e.result_summary || {};
      const resStr = Object.entries(summary).map(([k,v]) =>
        `${esc(k)}=${esc(String(v).substring(0,40))}`
      ).join(', ');

      const status = e.error
        ? `<span style="color:var(--red)" title="${esc(e.error)}">✗ Error</span>`
        : `<span style="color:var(--green)">✓ OK</span>`;

      const icon = e.client_id === 'chatgpt' ? '🤖' : e.client_id === 'claude-desktop' ? '🧠' : '🔌';

      html += `<tr style="cursor:pointer" onclick="showQueryDetail(${e.id}, this)" title="Click for details">
        <td style="white-space:nowrap">${fmtTime(e.created_at)}</td>
        <td>${icon} ${esc(e.client_name||e.client_id)}</td>
        <td><code style="color:var(--cyan)">${esc(e.tool_name)}</code></td>
        <td style="font-size:.8rem;max-width:250px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${argStr||'<span style="color:var(--muted)">—</span>'}</td>
        <td style="font-size:.8rem;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${resStr||`${e.result_count} results`}</td>
        <td>${e.duration_ms}ms</td>
        <td>${status}</td>
      </tr>`;
    }
    html += '</tbody></table>';
    if (!entries.length) html = '<div class="empty-state">No queries logged yet. Data will appear here once ChatGPT or Claude Desktop makes MCP tool calls.</div>';
    $('qlog-table').innerHTML = html;
  } catch(e) {
    $('qlog-table').innerHTML = '<div class="empty-state">Error: '+esc(e.message)+'</div>';
  }
}

function showQueryDetail(id, row) {
  // Toggle detail row
  const existing = row.nextElementSibling;
  if (existing && existing.classList.contains('query-detail-row')) {
    existing.remove();
    return;
  }
  // Find the entry data
  const cells = row.querySelectorAll('td');
  const detailRow = document.createElement('tr');
  detailRow.classList.add('query-detail-row');
  detailRow.innerHTML = `<td colspan="7" style="background:var(--surface);padding:12px;font-size:.85rem">
    <strong>Full Arguments:</strong> <pre style="background:var(--bg);padding:8px;border-radius:4px;margin:4px 0;overflow-x:auto;font-size:.8rem;max-height:200px">${esc(cells[3].title || cells[3].textContent)}</pre>
    <strong>Full Results:</strong> <pre style="background:var(--bg);padding:8px;border-radius:4px;margin:4px 0;overflow-x:auto;font-size:.8rem;max-height:200px">${esc(cells[4].title || cells[4].textContent)}</pre>
  </td>`;
  row.parentNode.insertBefore(detailRow, row.nextSibling);
}

function qlogPrev(){ qlogOffset = Math.max(0, qlogOffset - QLOG_LIMIT); loadQueryLog(); }
function qlogNext(){ qlogOffset += QLOG_LIMIT; loadQueryLog(); }

'''

    # Insert before </script>
    script_end = "\n</script>"
    if "loadMcpAccounts" not in source:
        source = source.replace(script_end, js_code + script_end)

    return source


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "/opt/wdws/dashboard/app.py"
    with open(path, "r") as f:
        original = f.read()

    patched = patch(original)

    if patched == original:
        print("No changes needed — already patched")
        sys.exit(0)

    # Backup
    with open(path + ".pre_mcp_accounts_backup", "w") as f:
        f.write(original)

    with open(path, "w") as f:
        f.write(patched)

    # Count changes
    has_api = "api_mcp_clients" in patched
    has_tabs = "tab-mcp" in patched
    has_js = "loadMcpAccounts" in patched
    has_routes = "/api/mcp/clients" in patched
    print(f"✓ Patched {path}")
    print(f"  - API endpoints: {'Yes' if has_api else 'No'}")
    print(f"  - Routes: {'Yes' if has_routes else 'No'}")
    print(f"  - HTML tabs: {'Yes' if has_tabs else 'No'}")
    print(f"  - JavaScript: {'Yes' if has_js else 'No'}")
