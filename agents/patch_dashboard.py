#!/usr/bin/env python3
"""
Dashboard patch — Enhanced Agents tab with fleet management.
Run on the server to patch /opt/wdws/dashboard/app.py
"""
import re, shutil, sys

DASH = "/opt/wdws/dashboard/app.py"
shutil.copy2(DASH, DASH + ".bak.agents")

with open(DASH, "r") as f:
    src = f.read()

# ── 1. Add new API endpoints after api_agent_sessions ─────────

NEW_API = '''

# ── Agent Runs (new agent system) ─────────────────────────────
async def api_agent_runs(request: Request):
    err = await _check_auth(request)
    if err:
        return err
    p = await get_pool()
    agent_id = request.query_params.get("agent_id")
    limit = min(int(request.query_params.get("limit", "50")), 200)
    if agent_id:
        rows = await p.fetch("""
            SELECT id, agent_id, status, started_at, finished_at,
                   duration_ms, summary, metrics, error_message
            FROM ops.agent_runs WHERE agent_id = $1
            ORDER BY started_at DESC LIMIT $2
        """, agent_id, limit)
    else:
        rows = await p.fetch("""
            SELECT id, agent_id, status, started_at, finished_at,
                   duration_ms, summary, metrics, error_message
            FROM ops.agent_runs ORDER BY started_at DESC LIMIT $1
        """, limit)
    return JSONResponse([_row_dict(r, ["metrics"]) for r in rows])


# ── Agent Findings ────────────────────────────────────────────
async def api_agent_findings(request: Request):
    err = await _check_auth(request)
    if err:
        return err
    p = await get_pool()
    status_filter = request.query_params.get("status", "open")
    severity = request.query_params.get("severity")
    limit = min(int(request.query_params.get("limit", "100")), 500)

    q = """SELECT id, agent_id, severity, category, title, description,
                  status, details, created_at, resolved_at
           FROM ops.agent_findings WHERE 1=1"""
    args = []
    n = 0
    if status_filter and status_filter != "all":
        n += 1; q += f" AND status = ${n}"; args.append(status_filter)
    if severity:
        n += 1; q += f" AND severity = ${n}"; args.append(severity)
    q += f" ORDER BY created_at DESC LIMIT ${n+1}"
    args.append(limit)
    rows = await p.fetch(q, *args)
    return JSONResponse([_row_dict(r, ["details"]) for r in rows])


async def api_agent_finding_resolve(request: Request):
    err = await _check_auth(request)
    if err:
        return err
    fid = request.path_params["id"]
    p = await get_pool()
    await p.execute("""
        UPDATE ops.agent_findings SET status = 'resolved', resolved_at = now()
        WHERE id = $1
    """, int(fid))
    return JSONResponse({"ok": True})


# ── Agent Trigger (run on demand) ─────────────────────────────
async def api_agent_trigger(request: Request):
    err = await _check_auth(request)
    if err:
        return err
    agent_id = request.path_params["agent_id"]
    import subprocess
    try:
        result = subprocess.Popen(
            ["/opt/wdws/venv/bin/python3", "/opt/wdws/agents/run.py", "--once", agent_id],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
        )
        return JSONResponse({"ok": True, "message": f"Agent {agent_id} triggered"})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


# ── Agent Toggle (enable/disable) ────────────────────────────
async def api_agent_toggle(request: Request):
    err = await _check_auth(request)
    if err:
        return err
    agent_id = request.path_params["agent_id"]
    p = await get_pool()
    row = await p.fetchrow("SELECT is_active FROM ops.agent_registry WHERE id = $1", agent_id)
    if not row:
        return JSONResponse({"error": "Agent not found"}, status_code=404)
    new_val = not row["is_active"]
    await p.execute("UPDATE ops.agent_registry SET is_active = $1 WHERE id = $2", new_val, agent_id)
    return JSONResponse({"ok": True, "is_active": new_val})

'''

# Insert after api_agent_sessions function
marker = "# ── Full-Text Search"
if marker in src:
    src = src.replace(marker, NEW_API + "\n" + marker)
else:
    print(f"WARNING: Could not find marker '{marker}' — inserting before api_search")
    src = src.replace("async def api_search(", NEW_API + "\nasync def api_search(")

# ── 2. Add routes ─────────────────────────────────────────────

new_routes = '''    Route("/api/agents/runs", api_agent_runs),
    Route("/api/agents/findings", api_agent_findings),
    Route("/api/agents/findings/{id:int}/resolve", api_agent_finding_resolve, methods=["POST"]),
    Route("/api/agents/{agent_id}/trigger", api_agent_trigger, methods=["POST"]),
    Route("/api/agents/{agent_id}/toggle", api_agent_toggle, methods=["POST"]),
'''

# Insert after the agents/sessions route
src = src.replace(
    '    Route("/api/agents/sessions", api_agent_sessions),',
    '    Route("/api/agents/sessions", api_agent_sessions),\n' + new_routes
)

# ── 3. Replace Agents tab HTML ────────────────────────────────

old_tab = '''<!-- ═══ AGENTS TAB ═══ -->
<section id="tab-agents" class="hidden">
  <div id="agents-list"></div>
  <div class="section-title" style="margin-top:24px">📝 Recent Sessions</div>
  <div id="agent-sessions"></div>
</section>'''

new_tab = '''<!-- ═══ AGENTS TAB ═══ -->
<section id="tab-agents" class="hidden">
  <div style="display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap;align-items:center">
    <button class="btn btn-outline btn-sm" onclick="loadAgents()">↻ Refresh</button>
    <select id="agent-finding-filter" onchange="loadAgentFindings()" style="font-size:.8rem;padding:4px 8px">
      <option value="open">Open Findings</option>
      <option value="all">All Findings</option>
      <option value="resolved">Resolved</option>
    </select>
    <select id="agent-severity-filter" onchange="loadAgentFindings()" style="font-size:.8rem;padding:4px 8px">
      <option value="">All Severities</option>
      <option value="critical">Critical</option>
      <option value="warning">Warning</option>
      <option value="info">Info</option>
    </select>
  </div>

  <div class="section-title">🤖 Agent Fleet</div>
  <div id="agents-fleet"></div>

  <div class="section-title" style="margin-top:24px">🚨 Findings <span id="findings-badge" style="font-size:.75rem;color:var(--muted);font-weight:400"></span></div>
  <div id="agents-findings"></div>

  <div class="section-title" style="margin-top:24px">📋 Recent Runs</div>
  <div id="agents-runs"></div>
</section>'''

src = src.replace(old_tab, new_tab)

# ── 4. Replace loadAgents JS function ─────────────────────────

old_js_start = "async function loadAgents(){"
old_js_end = "//  TAB: DATABASE"

# Find the old function boundaries
js_start_idx = src.index(old_js_start)
js_end_idx = src.index(old_js_end)

new_js = '''async function loadAgents(){
  try {
    const [agents, runs, findings] = await Promise.all([
      api('/api/agents'),
      api('/api/agents/runs?limit=30'),
      api('/api/agents/findings?status=' + ($('agent-finding-filter')?.value || 'open') + 
          '&severity=' + ($('agent-severity-filter')?.value || '') + '&limit=50')
    ]);

    // ── Fleet table ──
    $('agents-fleet').innerHTML = agents.length
      ? `<div class="tbl-wrap"><table>
        <thead><tr><th>Agent</th><th>P</th><th>Status</th><th>Schedule</th><th>Last Run</th><th>Runs</th><th>Errors</th><th>Actions</th></tr></thead>
        <tbody>${agents.sort((a,b)=>(a.priority||99)-(b.priority||99)).map(a=>{
          const errRate = a.run_count > 0 ? ((a.error_count||0)/a.run_count*100).toFixed(0) : 0;
          const statusDot = a.is_active 
            ? (a.last_status==='error'?'dot-yellow':'dot-green')
            : 'dot-red';
          const statusText = a.is_active 
            ? (a.last_status==='error'?'Error':'Active')
            : 'Disabled';
          return `<tr>
            <td><strong>${a.name||a.id}</strong><br><span style="font-size:.7rem;color:var(--muted)">${a.id}</span></td>
            <td style="text-align:center">${a.priority??'-'}</td>
            <td><span class="dot ${statusDot}"></span>${statusText}</td>
            <td style="font-family:monospace;font-size:.75rem">${a.schedule||'-'}</td>
            <td>${fmtTime(a.last_run_at)}</td>
            <td>${a.run_count||0}</td>
            <td>${(a.error_count||0)}${errRate>20?' <span style="color:var(--danger);font-size:.7rem">('+errRate+'%)</span>':''}</td>
            <td>
              <button class="btn btn-outline btn-sm" style="font-size:.7rem;padding:2px 6px" onclick="triggerAgent('${a.id}')">▶ Run</button>
              <button class="btn btn-outline btn-sm" style="font-size:.7rem;padding:2px 6px" onclick="toggleAgent('${a.id}')">${a.is_active?'⏸':'▶'}</button>
            </td>
          </tr>`}).join('')}</tbody></table></div>`
      : '<div class="empty-state">No agents registered. Deploy the agent system to get started.</div>';

    // ── Findings ──
    const sevColors = {critical:'var(--danger)',warning:'#f59e0b',info:'var(--accent)'};
    const sevIcons = {critical:'🔴',warning:'🟡',info:'🔵'};
    const openCount = findings.filter(f=>f.status==='open').length;
    $('findings-badge').textContent = openCount ? `(${openCount} open)` : '';
    
    $('agents-findings').innerHTML = findings.length
      ? `<div class="tbl-wrap"><table>
        <thead><tr><th>Sev</th><th>Agent</th><th>Category</th><th>Title</th><th>Time</th><th>Status</th><th></th></tr></thead>
        <tbody>${findings.map(f=>`<tr style="cursor:pointer" onclick="showFinding(${JSON.stringify(JSON.stringify(f))})">
          <td>${sevIcons[f.severity]||'⚪'}</td>
          <td style="font-size:.8rem">${f.agent_id}</td>
          <td><span class="badge badge-purple">${f.category||'-'}</span></td>
          <td>${f.title||'-'}</td>
          <td style="font-size:.8rem">${fmtTime(f.created_at)}</td>
          <td>${f.status==='open'?'<span class="badge badge-yellow">Open</span>':'<span class="badge badge-green">Resolved</span>'}</td>
          <td>${f.status==='open'?'<button class="btn btn-outline btn-sm" style="font-size:.65rem;padding:1px 5px" onclick="event.stopPropagation();resolveFinding('+f.id+')">✓</button>':''}</td>
        </tr>`).join('')}</tbody></table></div>`
      : '<div class="empty-state">No findings yet</div>';

    // ── Runs ──
    $('agents-runs').innerHTML = runs.length
      ? `<div class="tbl-wrap"><table>
        <thead><tr><th>Agent</th><th>Status</th><th>Started</th><th>Duration</th><th>Summary</th></tr></thead>
        <tbody>${runs.map(r=>{
          const dur = r.duration_ms ? (r.duration_ms/1000).toFixed(1)+'s' : '-';
          const sc = r.status==='completed'?'dot-green':r.status==='error'?'dot-red':'dot-yellow';
          return `<tr>
            <td style="font-size:.85rem">${r.agent_id}</td>
            <td><span class="dot ${sc}"></span>${r.status}</td>
            <td>${fmtTime(r.started_at)}</td>
            <td style="font-family:monospace;font-size:.8rem">${dur}</td>
            <td style="font-size:.8rem;max-width:400px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${r.summary||r.error_message||'-'}</td>
          </tr>`}).join('')}</tbody></table></div>`
      : '<div class="empty-state">No runs yet</div>';

  } catch(e) { $('agents-fleet').innerHTML='<div class="empty-state">Failed to load agents: '+e.message+'</div>'; }
}

function loadAgentFindings(){ loadAgents(); }

async function triggerAgent(id){
  if(!confirm('Run agent "'+id+'" now?')) return;
  try {
    await api('/api/agents/'+id+'/trigger','POST');
    showToast('Agent '+id+' triggered');
    setTimeout(loadAgents, 3000);
  } catch(e){ showToast('Failed: '+e.message,'error'); }
}

async function toggleAgent(id){
  try {
    const r = await api('/api/agents/'+id+'/toggle','POST');
    showToast('Agent '+id+': '+(r.is_active?'Enabled':'Disabled'));
    loadAgents();
  } catch(e){ showToast('Failed: '+e.message,'error'); }
}

async function resolveFinding(id){
  try {
    await api('/api/agents/findings/'+id+'/resolve','POST');
    showToast('Finding resolved');
    loadAgents();
  } catch(e){ showToast('Failed: '+e.message,'error'); }
}

function showFinding(jsonStr){
  const f = JSON.parse(jsonStr);
  const sevColors = {critical:'#ef4444',warning:'#f59e0b',info:'#3b82f6'};
  let detailsHtml = '';
  if(f.details){
    try {
      const d = typeof f.details === 'string' ? JSON.parse(f.details) : f.details;
      detailsHtml = '<pre style="background:#1a1a2e;padding:12px;border-radius:6px;overflow:auto;max-height:300px;font-size:.75rem;color:#e0e0e0">'+JSON.stringify(d,null,2)+'</pre>';
    } catch{ detailsHtml = '<pre style="background:#1a1a2e;padding:12px;border-radius:6px;font-size:.75rem">'+f.details+'</pre>'; }
  }
  const m = document.createElement('div');
  m.className = 'qd-modal';
  m.onclick = e=>{ if(e.target===m) m.remove(); };
  m.innerHTML = `<div class="qd-content" style="max-width:700px">
    <div class="qd-header">
      <h3 style="color:${sevColors[f.severity]||'#888'}">${f.severity?.toUpperCase()||'INFO'} — ${f.title||'Finding'}</h3>
      <button class="qd-close" onclick="this.closest('.qd-modal').remove()">&times;</button>
    </div>
    <div class="qd-grid">
      <div class="qd-card"><label>Agent</label><div>${f.agent_id}</div></div>
      <div class="qd-card"><label>Category</label><div>${f.category||'-'}</div></div>
      <div class="qd-card"><label>Status</label><div>${f.status}</div></div>
      <div class="qd-card"><label>Created</label><div>${fmtTime(f.created_at)}</div></div>
    </div>
    <div style="margin-top:12px"><label style="font-size:.75rem;color:var(--muted)">Description</label><p style="white-space:pre-wrap;font-size:.85rem">${f.description||'-'}</p></div>
    ${detailsHtml ? '<div style="margin-top:12px"><label style="font-size:.75rem;color:var(--muted)">Details</label>'+detailsHtml+'</div>' : ''}
    ${f.status==='open'?'<div style="margin-top:16px;text-align:right"><button class="btn btn-primary btn-sm" onclick="resolveFinding('+f.id+');this.closest(\\'.qd-modal\\').remove()">✓ Resolve</button></div>':''}
  </div>`;
  document.body.appendChild(m);
}

'''

# Replace from old function start to the TAB: DATABASE comment
replacement_target = src[js_start_idx:js_end_idx]
src = src[:js_start_idx] + new_js + src[js_end_idx:]

# ── 5. Update the agents API to include new columns ──────────

old_agents_query = """SELECT ar.id, ar.name, ar.description, ar.capabilities,
               ar.is_active, ar.created_at,
               COUNT(s.id)      AS session_count,
               MAX(s.started_at) AS last_session
        FROM ops.agent_registry ar
        LEFT JOIN ops.agent_sessions s ON ar.id = s.agent_id
        GROUP BY ar.id ORDER BY ar.created_at DESC"""

new_agents_query = """SELECT ar.id, ar.name, ar.description, ar.capabilities,
               ar.is_active, ar.created_at, ar.schedule, ar.priority,
               ar.last_run_at, ar.last_status, ar.run_count, ar.error_count,
               ar.agent_type, ar.version
        FROM ops.agent_registry ar
        ORDER BY ar.priority NULLS LAST, ar.name"""

src = src.replace(old_agents_query, new_agents_query)

# ── Write ──
with open(DASH, "w") as f:
    f.write(src)

print("✅ Dashboard patched successfully")
print(f"   Backup at: {DASH}.bak.agents")
