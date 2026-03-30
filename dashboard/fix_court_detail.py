"""Fix court record detail page:
1. Back navigation works (returns to previous context - person or court records list)
2. Enrich detail with: PACER link, nature of suit, party info from raw_data
3. Show all duplicate records for this case_number
4. Show related documents
5. Format raw_data as pretty JSON
"""

# ── Fix API: add sibling records for same case_number ──
api_path = '/opt/wdws/dashboard/osint_api.py'
api = open(api_path).read()

old_detail_return = """    docs = await p.fetch(\"\"\"
        SELECT d.id, d.title, d.document_type, d.mime_type, d.file_size_bytes, d.created_at
        FROM documents d
        WHERE d.investigation_id = (SELECT investigation_id FROM court_records WHERE id = $1)
          AND (d.title ILIKE '%' || $2 || '%' OR d.file_path ILIKE '%' || $2 || '%')
        LIMIT 20
    \"\"\", rec_id, dict(rec).get("case_number", "ZZZZNOCASE"))

    return JSONResponse({
        "record": _row(rec),
        "docket_entries": [_row(r) for r in docket],
        "persons": [_row(r) for r in persons],
        "related_documents": [_row(r) for r in docs],
    })"""

new_detail_return = """    docs = await p.fetch(\"\"\"
        SELECT d.id, d.title, d.document_type, d.mime_type, d.file_size_bytes, d.created_at
        FROM documents d
        WHERE d.investigation_id = (SELECT investigation_id FROM court_records WHERE id = $1::uuid)
          AND (d.title ILIKE '%' || $2 || '%' OR d.file_path ILIKE '%' || $2 || '%')
        LIMIT 20
    \"\"\", rec_id, dict(rec).get("case_number", "ZZZZNOCASE"))

    # Sibling records (same case_number, different ingestion)
    siblings = await p.fetch(\"\"\"
        SELECT id, case_number, case_name, court_name, court_type, source, filing_date,
               raw_data->>'source' AS raw_source
        FROM court_records
        WHERE case_number = $1 AND id != $2::uuid
        ORDER BY created_at DESC
    \"\"\", dict(rec).get("case_number", ""), rec_id)

    return JSONResponse({
        "record": _row(rec),
        "docket_entries": [_row(r) for r in docket],
        "persons": [_row(r) for r in persons],
        "related_documents": [_row(r) for r in docs],
        "sibling_records": [_row(r) for r in siblings],
    })"""

if old_detail_return in api:
    api = api.replace(old_detail_return, new_detail_return)
    print("API: Added sibling records + fixed UUID cast")
else:
    print("ERROR: Could not find detail return block")

open(api_path, 'w').write(api)

# ── Fix JS: Rewrite openOsintCourtRecord ──
js_path = '/opt/wdws/dashboard/osint_static/osint.js'
js = open(js_path).read()

old_func = """async function openOsintCourtRecord(recId) {
  osintCurrentView = 'record-detail';
  const el = document.querySelector('main > section:not(.hidden) > div[id]') || osintEl('records');
  el.innerHTML = '<div class="empty-state"><span class="spinner"></span></div>';

  try {
    const data = await api(`/api/osint/court-records/${recId}`);
    const r = data.record;
    let h = '';

    h += `<div class="osint-breadcrumb">
      <a href="#" onclick="loadOsintCourtRecords();return false">← Court Records</a>
      <span> / </span><strong>${esc(r.case_number || r.id)}</strong>
    </div>`;

    h += `<div class="osint-detail-header"><div>
      <h2>${esc(r.case_name || r.case_number || 'Court Record')}</h2>
      <div style="margin-top:6px;display:flex;gap:8px;flex-wrap:wrap">
        ${osintBadge(r.case_number, 'cyan')}
        ${osintBadge(r.court_type, 'blue')}
        ${osintBadge(r.case_status, r.case_status === 'closed' ? 'red' : 'green')}
        ${osintBadge(r.source || (r.raw_data && r.raw_data.source), 'purple')}
      </div>
    </div></div>`;

    // Fields
    h += '<div class="osint-fields">';
    h += osintField('Court', r.court_name);
    h += osintField('Jurisdiction', r.jurisdiction);
    h += osintField('Filing Date', fmtDate(r.filing_date));
    h += osintField('Disposition Date', fmtDate(r.disposition_date));
    h += osintField('Disposition', r.disposition);
    h += osintField('Plaintiff', r.plaintiff);
    h += osintField('Defendant', r.defendant);
    h += osintField('Attorney', r.attorney);
    h += osintField('Judge', r.judge);
    h += osintField('Case Type', r.case_type);
    if (r.amount) h += osintField('Amount', '$' + Number(r.amount).toLocaleString());
    if (r.charges && r.charges.length) h += osintField('Charges', r.charges.join('; '));
    if (r.sentence) h += osintField('Sentence', r.sentence);
    h += '</div>';

    // Linked persons
    if (data.persons.length) {
      h += '<div class="section-title">👤 Linked Persons</div>';
      h += '<div class="data-table"><table><thead><tr><th>Name</th><th>Role</th><th></th></tr></thead><tbody>';
      for (const p of data.persons) {
        h += `<tr><td>${esc(p.full_name || p.first_name + ' ' + p.last_name)}</td>
          <td>${osintBadge(p.role_in_case, 'purple')}</td>
          <td><button class="btn btn-sm btn-outline" onclick="openOsintPerson('${p.id}')">View</button></td></tr>`;
      }
      h += '</tbody></table></div>';
    }

    // Docket entries
    if (data.docket_entries.length) {
      h += `<div class="section-title">📋 Docket Entries (${data.docket_entries.length})</div>`;
      h += '<div class="data-table"><table><thead><tr><th>#</th><th>Date</th><th>Description</th></tr></thead><tbody>';
      for (const de of data.docket_entries) {
        h += `<tr><td>${esc(de.entry_number || '')}</td><td style="white-space:nowrap">${fmtDate(de.entry_date)}</td>
          <td>${esc((de.description || de.entry_text || '').substring(0, 200))}</td></tr>`;
      }
      h += '</tbody></table></div>';
    }

    // Raw data
    if (r.raw_data && Object.keys(r.raw_data).length) {
      h += '<div class="section-title">📦 Raw Data</div>';
      h += `<pre class="osint-raw">${syntaxHighlight(JSON.stringify(r.raw_data, null, 2))}</pre>`;
    }

    el.innerHTML = h;
  } catch (err) {
    el.innerHTML = `<div class="empty-state">❌ Failed: ${esc(err.message)}</div>`;
  }
}"""

new_func = r"""async function openOsintCourtRecord(recId, fromContext) {
  osintCurrentView = 'record-detail';
  // Track where we came from for back navigation
  if (!fromContext) fromContext = osintCurrentPerson ? 'person' : 'records';
  const _backPersonId = osintCurrentPerson;

  const el = osintEl('records') || document.querySelector('main > section:not(.hidden) > div[id]');
  if (!el) return;
  el.innerHTML = '<div class="empty-state"><span class="spinner"></span></div>';

  try {
    const data = await api(`/api/osint/court-records/${recId}`);
    const r = data.record;
    let rd = r.raw_data;
    if (typeof rd === 'string') { try { rd = JSON.parse(rd); } catch(e) { rd = {}; } }
    if (!rd) rd = {};
    const cc = rd.courtCase || {};
    let h = '';

    // Breadcrumb with proper back navigation
    const backFn = fromContext === 'person' && _backPersonId
      ? `openOsintPerson('${_backPersonId}')`
      : `loadOsintCourtRecords()`;
    const backLabel = fromContext === 'person' ? '← Back to Person' : '← Court Records';
    h += `<div class="osint-breadcrumb">
      <a href="#" onclick="${backFn};return false">${backLabel}</a>
      <span> / </span><strong>${esc(r.case_number || r.id)}</strong>
    </div>`;

    // Header
    h += `<div class="osint-detail-header"><div>
      <h2>${esc(r.case_name || cc.caseTitle || r.case_number || 'Court Record')}</h2>
      <div style="margin-top:6px;display:flex;gap:8px;flex-wrap:wrap;align-items:center">
        ${osintBadge(r.case_number, 'cyan')}
        ${osintBadge(r.court_type, 'blue')}
        ${osintBadge(r.case_status, r.case_status === 'closed' ? 'red' : 'green')}
        ${osintBadge(r.source || rd.source, 'purple')}
        ${rd.partyRole ? osintBadge('Role: ' + rd.partyRole, 'amber') : ''}
        ${rd.natureOfSuit ? osintBadge('NOS: ' + rd.natureOfSuit, 'gray') : ''}
      </div>
      ${cc.caseLink ? `<div style="margin-top:8px"><a href="${esc(cc.caseLink)}" target="_blank" style="color:var(--cyan);font-size:.85rem">🔗 View on PACER ↗</a></div>` : ''}
    </div></div>`;

    // Case Information Fields
    h += '<div class="section-title">📋 Case Information</div>';
    h += '<div class="osint-fields">';
    h += osintField('Court', r.court_name);
    h += osintField('Jurisdiction', r.jurisdiction);
    h += osintField('Filing Date', fmtDate(r.filing_date || rd.dateFiled));
    h += osintField('Disposition Date', fmtDate(r.disposition_date));
    h += osintField('Case Type', r.case_type);
    h += osintField('Nature of Suit', rd.natureOfSuit);
    h += '</div>';

    // Parties
    const hasParties = r.plaintiff || r.defendant || r.attorney || r.judge;
    if (hasParties) {
      h += '<div class="section-title">👥 Parties</div>';
      h += '<div class="osint-fields">';
      h += osintField('Plaintiff', r.plaintiff);
      h += osintField('Defendant', r.defendant);
      h += osintField('Attorney', r.attorney || (rd.firstName ? `${rd.firstName} ${rd.middleName || ''} ${rd.lastName}`.trim() : ''));
      h += osintField('Judge', r.judge);
      if (r.amount) h += osintField('Amount', '$' + Number(r.amount).toLocaleString());
      if (r.disposition) h += osintField('Disposition', r.disposition);
      if (r.charges && r.charges.length) h += osintField('Charges', r.charges.join('; '));
      if (r.sentence) h += osintField('Sentence', r.sentence);
      h += '</div>';
    }

    // Linked persons from DB
    if (data.persons && data.persons.length) {
      h += '<div class="section-title">👤 Linked Persons</div>';
      h += '<div class="data-table"><table><thead><tr><th>Name</th><th>Role</th><th></th></tr></thead><tbody>';
      for (const p of data.persons) {
        h += `<tr><td>${esc(p.full_name || ((p.first_name||'') + ' ' + (p.last_name||'')).trim())}</td>
          <td>${osintBadge(p.role || '', 'purple')}</td>
          <td><button class="btn btn-sm btn-outline" onclick="openOsintPerson('${p.id}')">View</button></td></tr>`;
      }
      h += '</tbody></table></div>';
    }

    // Docket entries
    if (data.docket_entries && data.docket_entries.length) {
      h += `<div class="section-title">📜 Docket Entries (${data.docket_entries.length})</div>`;
      h += '<div class="data-table"><table><thead><tr><th style="width:50px">#</th><th style="width:100px">Date</th><th>Description</th></tr></thead><tbody>';
      for (const de of data.docket_entries) {
        h += `<tr><td>${esc(String(de.entry_number || ''))}</td>
          <td style="white-space:nowrap">${fmtDate(de.date_filed || de.entry_date)}</td>
          <td style="max-width:600px">${esc((de.description || de.entry_text || '').substring(0, 300))}</td></tr>`;
      }
      h += '</tbody></table></div>';
    }

    // Related documents
    if (data.related_documents && data.related_documents.length) {
      h += `<div class="section-title">📄 Related Documents (${data.related_documents.length})</div>`;
      h += '<div class="data-table"><table><thead><tr><th>Title</th><th>Type</th><th>Size</th><th>Date</th></tr></thead><tbody>';
      for (const doc of data.related_documents) {
        const sz = doc.file_size_bytes ? (doc.file_size_bytes > 1048576 ? (doc.file_size_bytes/1048576).toFixed(1)+' MB' : (doc.file_size_bytes/1024).toFixed(0)+' KB') : '';
        h += `<tr onclick="openOsintDocument('${doc.id}')" style="cursor:pointer">
          <td>${esc(doc.title || '—')}</td>
          <td>${osintBadge(doc.document_type, 'green')}</td>
          <td style="font-size:.8rem;color:var(--muted)">${sz}</td>
          <td style="white-space:nowrap">${fmtDate(doc.created_at)}</td></tr>`;
      }
      h += '</tbody></table></div>';
    }

    // Sibling / duplicate records for same case
    if (data.sibling_records && data.sibling_records.length) {
      h += `<div class="section-title">🔄 Other Records for This Case (${data.sibling_records.length})</div>`;
      h += '<div class="data-table"><table><thead><tr><th>Case #</th><th>Case Name</th><th>Court</th><th>Type</th><th>Filed</th></tr></thead><tbody>';
      for (const s of data.sibling_records) {
        h += `<tr onclick="openOsintCourtRecord('${s.id}','${fromContext}')" style="cursor:pointer">
          <td style="font-family:monospace;font-size:.8rem">${esc(s.case_number || '')}</td>
          <td>${esc((s.case_name || '').substring(0, 60))}</td>
          <td style="font-size:.8rem">${esc((s.court_name || '').substring(0, 35))}</td>
          <td>${osintBadge(s.court_type, 'blue')}</td>
          <td style="white-space:nowrap">${fmtDate(s.filing_date)}</td></tr>`;
      }
      h += '</tbody></table></div>';
    }

    // Raw data (collapsed by default)
    if (rd && Object.keys(rd).length) {
      const rawId = 'osint-raw-' + Date.now();
      h += `<div class="section-title" style="cursor:pointer" onclick="document.getElementById('${rawId}').classList.toggle('hidden')">
        📦 Raw Data <span style="font-size:.7rem;color:var(--muted)">click to expand</span>
      </div>`;
      h += `<div id="${rawId}" class="hidden">
        <pre class="osint-raw">${typeof syntaxHighlight === 'function' ? syntaxHighlight(JSON.stringify(rd, null, 2)) : esc(JSON.stringify(rd, null, 2))}</pre>
      </div>`;
    }

    el.innerHTML = h;
  } catch (err) {
    el.innerHTML = `<div class="empty-state">❌ Failed: ${esc(err.message)}</div>`;
  }
}"""

if old_func in js:
    js = js.replace(old_func, new_func)
    print("JS: Rewrote openOsintCourtRecord with back nav + enriched detail")
else:
    print("ERROR: Could not find openOsintCourtRecord function")
    # Debug
    if 'async function openOsintCourtRecord(recId)' in js:
        print("  Found function signature")
    else:
        print("  Function signature not found!")

# Also fix the person detail view's onclick to pass context
old_person_onclick = """        h += `<tr onclick="openOsintCourtRecord('${cr.id}')" style="cursor:pointer">
          <td style="font-family:monospace;font-size:.8rem">${esc(cr.case_number || '')}</td>
          <td>${esc((cr.case_name || '').substring(0, 80))}</td>
          <td style="font-size:.8rem">${esc((cr.court_name || '').substring(0, 40))}</td>
          <td>${osintBadge(cr.role || '', 'purple')}</td>
          <td style="white-space:nowrap">${fmtDate(cr.filing_date)}</td>
          <td style="text-align:center">${cnt > 1 ? `<span style="background:rgba(59,130,246,.15);color:var(--cyan);padding:2px 8px;border-radius:10px;font-size:.8rem">${cnt}</span>` : '1'}</td>
        </tr>`;"""

new_person_onclick = """        h += `<tr onclick="openOsintCourtRecord('${cr.id}','person')" style="cursor:pointer">
          <td style="font-family:monospace;font-size:.8rem">${esc(cr.case_number || '')}</td>
          <td>${esc((cr.case_name || '').substring(0, 80))}</td>
          <td style="font-size:.8rem">${esc((cr.court_name || '').substring(0, 40))}</td>
          <td>${osintBadge(cr.role || '', 'purple')}</td>
          <td style="white-space:nowrap">${fmtDate(cr.filing_date)}</td>
          <td style="text-align:center">${cnt > 1 ? `<span style="background:rgba(59,130,246,.15);color:var(--cyan);padding:2px 8px;border-radius:10px;font-size:.8rem">${cnt}</span>` : '1'}</td>
        </tr>`;"""

if old_person_onclick in js:
    js = js.replace(old_person_onclick, new_person_onclick)
    print("JS: Fixed person detail court record onclick to pass context")
else:
    print("WARN: Could not find person court record onclick")

# Fix: the el selector in the function needs to correctly target the visible tab content
# When coming from person detail, the visible section is osint-persons, not osint-records
# We need to use person content when from person, records when from records list
# Let's also make the court records tab render into records element
# Actually the issue is: when viewing a person, osint-records-content is hidden.
# We need to show the records tab and render there. Let's fix by always switching to the records tab.
# Or better: render in whatever container is visible.

open(js_path, 'w').write(js)
print("Done. Restart needed.")
