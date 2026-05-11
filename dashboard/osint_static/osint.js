// ═══════════════════════════════════════════════════════════
//  OSINT INVESTIGATIONS MODULE — Athena Cognitive Platform
// ═══════════════════════════════════════════════════════════
// Provides UI for browsing the OSINT Investigation Database
// (osint-db): Investigations, Persons, Court Records, Documents, Properties

// ── State ────────────────────────────────────────────────
let osintCurrentInv = null;
let osintCourtOffset = 0;
let osintDocOffset = 0;
let osintCurrentPerson = null;
let osintCurrentView = 'inv-list';      // inv-list | inv-detail | person-detail | record-detail | property-detail

// ── Helpers (reuse global fns from main app) ─────────────
function osintBadge(text, color='blue') {
  if (!text) return '';
  return `<span class="badge badge-${color}">${esc(text)}</span>`;
}
function fmtDate(iso) {
  if (!iso) return '—';
  try { return new Date(iso).toLocaleDateString('en-US', {year:'numeric', month:'short', day:'numeric'}); }
  catch { return iso; }
}
function fmtBytes(b) {
  if (!b) return '—';
  if (b > 1048576) return (b/1048576).toFixed(1) + ' MB';
  if (b > 1024) return (b/1024).toFixed(1) + ' KB';
  return b + ' B';
}
function osintField(label, val) {
  if (val === null || val === undefined || val === '') return '';
  return `<div class="field"><label>${esc(label)}</label><div class="val">${val}</div></div>`;
}

// ═══════════════════════════════════════════════════════════
// TAB: INVESTIGATIONS OVERVIEW
// ═══════════════════════════════════════════════════════════

function osintEl(tab) {
  // Return the content container for the given OSINT tab
  const map = {
    'inv': 'osint-content',
    'persons': 'osint-persons-content',
    'records': 'osint-records-content',
    'docs': 'osint-docs-content',
    'props': 'osint-props-content',
  };
  return $(map[tab] || 'osint-content');
}

async function loadOsintInvestigations() {
  osintCurrentView = 'inv-list';
  const el = osintEl('inv');
  el.innerHTML = '<div class="empty-state"><span class="spinner"></span></div>';
  // Clear any stale record-id state from a prior openOsintCourtRecord(recId) call.
  // This is the list view (no specific record). The reopen logic later in the
  // file checks `el.dataset.currentRecordId` truthiness, so deleting is correct.
  delete el.dataset.currentRecordId;

  try {
    const [stats, invs, sources] = await Promise.all([
      api('/api/osint/stats'),
      api('/api/osint/investigations'),
      api('/api/osint/sources'),
    ]);

    let h = '';

    // ── Stats Cards ──
    h += '<div class="cards">';
    h += osintStatCard('Investigations', stats.investigations, 'cyan');
    h += osintStatCard('Persons', stats.persons, 'purple');
    h += osintStatCard('Court Records', stats.court_records, 'blue', `${fmtNum(stats.unique_cases)} unique cases`);
    h += osintStatCard('Documents', stats.documents, 'green');
    h += osintStatCard('Properties', stats.properties, 'amber');
    h += osintStatCard('Timeline Events', stats.timeline_events, 'cyan');
    h += osintStatCard('Relationships', stats.entity_relationships, 'purple');
    h += osintStatCard('Notes', stats.notes, 'blue');
    h += '</div>';

    // ── Source & Type Breakdown ──
    h += '<div class="cols-2">';
    h += '<div class="bar-chart"><h3>Court Records by Source</h3>';
    if (sources.by_source.length) {
      const max = Math.max(...sources.by_source.map(s => s.cnt));
      h += sources.by_source.map((s, i) =>
        `<div class="bar-row">
          <span class="bar-label">${esc(s.source)}</span>
          <div class="bar" style="flex:1;height:18px;background:var(--surface2);border-radius:4px;overflow:hidden">
            <div style="width:${(s.cnt/max*100).toFixed(1)}%;height:100%;background:${barColor(i)};border-radius:4px"></div>
          </div>
          <span style="font-size:.8rem;color:var(--muted);min-width:40px;text-align:right">${fmtNum(s.cnt)}</span>
        </div>`
      ).join('');
    }
    h += '</div>';

    h += '<div class="bar-chart"><h3>Documents by Type</h3>';
    if (sources.by_doc_type.length) {
      const max = Math.max(...sources.by_doc_type.map(s => s.cnt));
      h += sources.by_doc_type.map((s, i) =>
        `<div class="bar-row">
          <span class="bar-label">${esc(s.doc_type)}</span>
          <div class="bar" style="flex:1;height:18px;background:var(--surface2);border-radius:4px;overflow:hidden">
            <div style="width:${(s.cnt/max*100).toFixed(1)}%;height:100%;background:${barColor(i)};border-radius:4px"></div>
          </div>
          <span style="font-size:.8rem;color:var(--muted);min-width:40px;text-align:right">${fmtNum(s.cnt)}</span>
        </div>`
      ).join('');
    }
    h += '</div></div>';

    // ── Investigation Cards ──
    h += '<div class="section-title">📂 Active Investigations</div>';
    h += '<div class="osint-inv-grid">';
    for (const inv of invs) {
      h += `<div class="osint-inv-card" onclick="openOsintInvestigation('${inv.id}')">
        <div class="osint-inv-header">
          <span class="osint-inv-case">${esc(inv.case_number)}</span>
          ${osintBadge(inv.status, inv.status === 'active' ? 'green' : 'amber')}
        </div>
        <div class="osint-inv-title">${esc(inv.title)}</div>
        ${inv.description ? `<div class="osint-inv-desc">${esc(inv.description).substring(0, 120)}</div>` : ''}
        <div class="osint-inv-stats">
          <span>📄 ${fmtNum(inv.doc_count)} docs</span>
          <span>⚖️ ${fmtNum(inv.record_count)} records</span>
          <span>👤 ${fmtNum(inv.entity_count)} entities</span>
          <span>📝 ${fmtNum(inv.note_count)} notes</span>
        </div>
        <div class="osint-inv-date">Created ${fmtDate(inv.created_at)}</div>
      </div>`;
    }
    h += '</div>';

    el.innerHTML = h;
  } catch (err) {
    el.innerHTML = `<div class="empty-state">❌ Failed to load investigations: ${esc(err.message)}</div>`;
  }
}

function osintStatCard(label, value, color, sub) {
  return `<div class="card"><div class="label">${label}</div>
    <div class="value ${color}">${fmtNum(value)}</div>
    ${sub ? `<div class="sub">${sub}</div>` : ''}</div>`;
}


// ═══════════════════════════════════════════════════════════
// INVESTIGATION DETAIL
// ═══════════════════════════════════════════════════════════

async function openOsintInvestigation(invId) {
  osintCurrentInv = invId;
  osintCurrentView = 'inv-detail';
  const el = osintEl('inv');
  el.innerHTML = '<div class="empty-state"><span class="spinner"></span></div>';

  try {
    const data = await api(`/api/osint/investigations/${invId}`);
    const inv = data.investigation;
    let h = '';

    // Breadcrumb
    h += `<div class="osint-breadcrumb">
      <a href="#" onclick="switchTab('osint-inv');return false">← All Investigations</a>
      <span> / </span>
      <strong>${esc(inv.case_number)} — ${esc(inv.title)}</strong>
    </div>`;

    // Header
    h += `<div class="osint-detail-header">
      <div>
        <h2>${esc(inv.title)}</h2>
        <div style="margin-top:6px">
          ${osintBadge(inv.case_number, 'cyan')}
          ${osintBadge(inv.status, inv.status === 'active' ? 'green' : 'amber')}
          ${inv.priority ? osintBadge('P' + inv.priority, 'purple') : ''}
          ${inv.lead_investigator ? osintBadge(inv.lead_investigator, 'blue') : ''}
        </div>
        ${inv.description ? `<p style="color:var(--muted);margin-top:8px;font-size:.9rem">${esc(inv.description)}</p>` : ''}
      </div>
    </div>`;

    // Quick Stats
    h += '<div class="cards" style="margin-top:16px">';
    h += osintStatCard('Documents', data.doc_stats.reduce((a,b) => a+b.cnt, 0), 'green');
    h += osintStatCard('Persons', data.persons.length, 'purple');
    h += osintStatCard('Timeline', data.timeline.length, 'cyan');
    h += osintStatCard('Notes', data.notes.length, 'blue');
    h += '</div>';

    // Source & Doc Type breakdown
    h += '<div class="cols-2" style="margin-top:16px">';
    if (data.record_stats.length) {
      h += '<div class="bar-chart"><h3>Records by Source</h3>';
      const max = Math.max(...data.record_stats.map(s => s.cnt));
      h += data.record_stats.map((s, i) =>
        `<div class="bar-row"><span class="bar-label">${esc(s.source)}</span>
          <div class="bar" style="flex:1;height:18px;background:var(--surface2);border-radius:4px;overflow:hidden">
            <div style="width:${(s.cnt/max*100).toFixed(1)}%;height:100%;background:${barColor(i)};border-radius:4px"></div>
          </div>
          <span style="font-size:.8rem;color:var(--muted);min-width:40px;text-align:right">${fmtNum(s.cnt)}</span></div>`
      ).join('');
      h += '</div>';
    }
    if (data.doc_stats.length) {
      h += '<div class="bar-chart"><h3>Documents by Type</h3>';
      const max = Math.max(...data.doc_stats.map(s => s.cnt));
      h += data.doc_stats.map((s, i) =>
        `<div class="bar-row"><span class="bar-label">${esc(s.document_type)}</span>
          <div class="bar" style="flex:1;height:18px;background:var(--surface2);border-radius:4px;overflow:hidden">
            <div style="width:${(s.cnt/max*100).toFixed(1)}%;height:100%;background:${barColor(i)};border-radius:4px"></div>
          </div>
          <span style="font-size:.8rem;color:var(--muted);min-width:40px;text-align:right">${fmtNum(s.cnt)}</span></div>`
      ).join('');
      h += '</div>';
    }
    h += '</div>';

    // ── Persons ──
    if (data.persons.length) {
      h += '<div class="section-title">👤 Subjects / Entities</div>';
      h += '<div class="data-table"><table><thead><tr><th>Name</th><th>Role</th><th>DOB</th><th>Occupation</th><th></th></tr></thead><tbody>';
      for (const p of data.persons) {
        h += `<tr>
          <td><strong>${esc(p.full_name || (p.first_name + ' ' + p.last_name))}</strong></td>
          <td>${osintBadge(p.role, 'purple')}</td>
          <td>${fmtDate(p.date_of_birth)}</td>
          <td>${esc(p.occupation || '')}</td>
          <td><button class="btn btn-sm btn-outline" onclick="openOsintPerson('${p.id}')">View Profile</button></td>
        </tr>`;
      }
      h += '</tbody></table></div>';
    }

    // ── Recent Documents ──
    if (data.recent_documents.length) {
      h += '<div class="section-title">📄 Recent Documents</div>';
      h += renderDocTable(data.recent_documents);
    }

    // ── Timeline ──
    if (data.timeline.length) {
      h += '<div class="section-title">📅 Timeline</div>';
      h += '<div class="osint-timeline">';
      for (const ev of data.timeline) {
        h += `<div class="osint-timeline-item">
          <div class="osint-timeline-date">${fmtDate(ev.event_date)}</div>
          <div class="osint-timeline-content">
            <strong>${esc(ev.event_type || '')}</strong>
            <span>${esc(ev.description || ev.title || '')}</span>
          </div>
        </div>`;
      }
      h += '</div>';
    }

    // ── Notes ──
    if (data.notes.length) {
      h += '<div class="section-title">📝 Notes</div>';
      for (const n of data.notes) {
        h += `<div class="osint-note">
          <div class="osint-note-header">${esc(n.title || 'Note')} · ${fmtDate(n.created_at)} · ${esc(n.created_by || '')}</div>
          <div class="osint-note-body">${esc(n.content || '')}</div>
        </div>`;
      }
    }

    el.innerHTML = h;
  } catch (err) {
    el.innerHTML = `<div class="empty-state">❌ Failed: ${esc(err.message)}</div>`;
  }
}


// ═══════════════════════════════════════════════════════════
// PERSON DETAIL
// ═══════════════════════════════════════════════════════════

async function openOsintPerson(personId) {
  osintCurrentPerson = personId;
  osintCurrentView = 'person-detail';
  // Person detail can be opened from multiple tabs — render into whatever is visible
  const el = document.querySelector('main > section:not(.hidden) > div[id]') || osintEl('persons');
  el.innerHTML = '<div class="empty-state"><span class="spinner"></span></div>';

  try {
    const data = await api(`/api/osint/persons/${personId}`);
    const p = data.person;
    let h = '';

    // Breadcrumb — back button goes to correct context
    const backAction = osintCurrentInv
      ? `openOsintInvestigation('${osintCurrentInv}');return false`
      : `switchTab('osint-persons');return false`;
    const backLabel = osintCurrentInv ? '← Investigation' : '← Persons';
    h += `<div class="osint-breadcrumb">
      <a href="#" onclick="${backAction}">${backLabel}</a>`;
    if (osintCurrentInv) {
      h += ` <span>/</span> <a href="#" onclick="openOsintInvestigation('${osintCurrentInv}');return false">Investigation</a>`;
    }
    h += ` <span>/</span> <strong>${esc(p.full_name || p.first_name + ' ' + p.last_name)}</strong></div>`;

    // Profile header
    h += `<div class="osint-person-header">
      <div class="osint-person-avatar">👤</div>
      <div>
        <h2>${esc(p.full_name || (p.first_name + ' ' + (p.middle_name || '') + ' ' + p.last_name))}</h2>
        <div style="margin-top:4px;display:flex;gap:8px;flex-wrap:wrap">
          ${p.occupation ? osintBadge(p.occupation, 'blue') : ''}
          ${p.gender && p.gender !== 'unknown' ? osintBadge(p.gender, 'purple') : ''}
          ${p.date_of_birth ? osintBadge('DOB: ' + fmtDate(p.date_of_birth), 'cyan') : ''}
          ${p.confidence ? osintBadge(p.confidence, p.confidence === 'verified' ? 'green' : 'amber') : ''}
        </div>
        ${p.bio ? `<p style="color:var(--muted);margin-top:8px;font-size:.9rem">${esc(p.bio).substring(0,300)}</p>` : ''}
        ${p.aliases && p.aliases.length ? `<div style="margin-top:4px;font-size:.8rem;color:var(--muted)">AKA: ${p.aliases.map(a => esc(a)).join(', ')}</div>` : ''}
      </div>
    </div>`;

    // Quick stat cards
    h += '<div class="cards" style="margin-top:16px">';
    h += osintStatCard('Court Records', data.court_records.length, 'blue');
    h += osintStatCard('Properties', data.properties.length, 'amber');
    h += osintStatCard('Addresses', data.addresses.length, 'cyan');
    h += osintStatCard('Investigations', data.investigations.length, 'purple');
    h += '</div>';

    // ── Investigations ──
    if (data.investigations.length) {
      h += '<div class="section-title">📂 Linked Investigations</div>';
      h += '<div class="data-table"><table><thead><tr><th>Case #</th><th>Title</th><th>Role</th><th>Status</th></tr></thead><tbody>';
      for (const inv of data.investigations) {
        h += `<tr onclick="openOsintInvestigation('${inv.id}')" style="cursor:pointer">
          <td>${esc(inv.case_number)}</td><td>${esc(inv.title)}</td>
          <td>${osintBadge(inv.role, 'purple')}</td><td>${osintBadge(inv.status, 'green')}</td></tr>`;
      }
      h += '</tbody></table></div>';
    }

    // ── Contact Info ──
    if (data.emails.length || data.phones.length || data.addresses.length) {
      h += '<div class="section-title">📇 Contact Information</div>';
      h += '<div class="osint-contact-grid">';
      if (data.emails.length) {
        h += '<div class="osint-contact-section"><h4>📧 Emails</h4>';
        for (const e of data.emails) {
          const etype = e.email_type || (e.raw_data && e.raw_data.email_type) || '';
          const bestMatch = e.is_best_match || (e.raw_data && e.raw_data.is_best_match) || false;
          h += `<div class="osint-contact-item">${esc(e.email)} ${etype ? osintBadge(etype, 'cyan') : ''}${bestMatch ? ' <span style="color:var(--gold);font-size:.9rem" title="Best Match">⭐</span>' : ''}</div>`;
        }
        h += '</div>';
      }
      if (data.phones.length) {
        h += '<div class="osint-contact-section"><h4>📞 Phones</h4>';
        for (const ph of data.phones) {
          const ptype = ph.phone_type && ph.phone_type !== 'unknown' ? ph.phone_type : '';
          h += `<div class="osint-contact-item">${esc(ph.phone_number)} ${ptype ? osintBadge(ptype, 'cyan') : ''}${ph.pp_is_current ? ' ' + osintBadge('current', 'green') : ''}</div>`;
        }
        h += '</div>';
      }
      if (data.addresses.length) {
        h += '<div class="osint-contact-section"><h4>🏠 Addresses</h4>';
        for (const a of data.addresses) {
          const addr = esc(a.full_address || [a.street, a.city, a.state, a.zip_code].filter(Boolean).join(', '));
          const atype = a.pa_address_type || a.address_type || '';
          const lastSeen = a.pa_date_last_seen || a.date_last_seen || '';
          const isCurrent = a.pa_is_current;
          let meta = '';
          if (isCurrent) meta += osintBadge('current', 'green');
          if (atype) meta += ' ' + osintBadge(atype, 'purple');
          if (lastSeen) meta += ` <span style="color:var(--muted);font-size:.78rem">${lastSeen.substring(0,7)}</span>`;
          h += `<div class="osint-contact-item">${addr} ${meta}</div>`;
        }
        h += '</div>';
      }
      h += '</div>';
    }

    // ── Employment ──
    if (data.employment.length) {
      h += '<div class="section-title">💼 Employment History</div>';
      h += '<div class="data-table"><table><thead><tr><th>Employer</th><th>Title</th><th>Start</th><th>End</th></tr></thead><tbody>';
      for (const e of data.employment) {
        h += `<tr><td>${esc(e.employer || e.company_name || '')}</td><td>${esc(e.title || e.position || '')}</td>
          <td>${fmtDate(e.start_date)}</td><td>${fmtDate(e.end_date)}</td></tr>`;
      }
      h += '</tbody></table></div>';
    }

    // ── Education ──
    if (data.education.length) {
      h += '<div class="section-title">🎓 Education</div>';
      h += '<div class="data-table"><table><thead><tr><th>Institution</th><th>Degree</th><th>Field</th><th>Year</th></tr></thead><tbody>';
      for (const e of data.education) {
        h += `<tr><td>${esc(e.institution || '')}</td><td>${esc(e.degree || '')}</td>
          <td>${esc(e.field_of_study || '')}</td><td>${esc(e.graduation_year || '')}</td></tr>`;
      }
      h += '</tbody></table></div>';
    }

    // ── Social Media ──
    if (data.social_media.length) {
      const platformIcons = {facebook:'📘',linkedin:'🔗',x:'𝕏',twitter:'𝕏',instagram:'📷',snapchat:'👻',pinterest:'📌',github:'🐙',youtube:'▶️',tiktok:'🎵',reddit:'🔶'};
      const knownPlatforms = ['facebook','linkedin','x','twitter','instagram','snapchat','pinterest','github','youtube','tiktok','reddit'];
      const profiles = data.social_media.filter(s => knownPlatforms.includes(s.platform) || (s.platform !== 'other' && s.username));
      const otherLinks = data.social_media.filter(s => !profiles.includes(s));

      if (profiles.length) {
        h += '<div class="section-title">🌐 Social Profiles</div>';
        h += '<div class="osint-social-profiles">';
        for (const s of profiles) {
          const icon = platformIcons[s.platform] || '🌐';
          const name = s.username || s.display_name || '—';
          const url = s.profile_url || '#';
          h += `<a href="${esc(url)}" target="_blank" class="osint-social-chip" title="${esc(url)}">
            <span class="osint-social-icon">${icon}</span>
            <span class="osint-social-platform">${esc(s.platform)}</span>
            <span class="osint-social-name">${esc(name)}</span>
          </a>`;
        }
        h += '</div>';
      }

      if (otherLinks.length) {
        const linkId = 'osint-other-links-' + Date.now();
        h += `<div class="section-title" style="cursor:pointer" onclick="document.getElementById('${linkId}').classList.toggle('hidden')">
          🔗 Related Links (${otherLinks.length}) <span style="font-size:.7rem;color:var(--muted)">click to expand</span>
        </div>`;
        h += `<div id="${linkId}" class="hidden">`;
        h += '<div class="data-table"><table><thead><tr><th style="width:50px">Type</th><th>URL / Reference</th></tr></thead><tbody>';
        for (const s of otherLinks) {
          const url = s.profile_url || '';
          let domain = '';
          try { domain = new URL(url).hostname.replace('www.',''); } catch(e) { domain = url.substring(0,40); }
          const label = s.username || s.display_name || domain || url.substring(0,60);
          h += `<tr>
            <td>${osintBadge(s.platform, 'gray')}</td>
            <td style="max-width:600px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">
              ${url ? `<a href="${esc(url)}" target="_blank" title="${esc(url)}" style="color:var(--accent)">${esc(label)}</a>` : esc(label)}
            </td></tr>`;
        }
        h += '</tbody></table></div></div>';
      }
    }

    // ── Court Records (deduplicated) ──
    if (data.court_records.length) {
      h += `<div class="section-title">⚖️ Court Records (${data.court_records.length} cases)</div>`;
      h += '<div class="data-table"><table><thead><tr><th>Case #</th><th>Case Name</th><th>Court</th><th>Role</th><th>Filed</th><th>Recs</th></tr></thead><tbody>';
      for (const cr of data.court_records.slice(0, 100)) {
        const cnt = cr.record_count || 1;
        h += `<tr onclick="openOsintCourtRecord('${cr.id}','person')" style="cursor:pointer">
          <td style="font-family:monospace;font-size:.8rem">${esc(cr.case_number || '')}</td>
          <td>${esc((cr.case_name || '').substring(0, 80))}</td>
          <td style="font-size:.8rem">${esc((cr.court_name || '').substring(0, 40))}</td>
          <td>${osintBadge(cr.role || '', 'purple')}</td>
          <td style="white-space:nowrap">${fmtDate(cr.filing_date)}</td>
          <td style="text-align:center">${cnt > 1 ? `<span style="background:rgba(59,130,246,.15);color:var(--cyan);padding:2px 8px;border-radius:10px;font-size:.8rem">${cnt}</span>` : '1'}</td>
        </tr>`;
      }
      h += '</tbody></table></div>';
      if (data.court_records.length > 100) {
        h += `<div style="color:var(--muted);font-size:.8rem;margin-top:8px">Showing 100 of ${data.court_records.length} cases</div>`;
      }
    }

    // ── Properties ──
    if (data.properties.length) {
      h += '<div class="section-title">🏠 Properties</div>';
      h += '<div class="data-table"><table><thead><tr><th>Address</th><th>Type</th><th>Relationship</th><th>Since</th><th></th></tr></thead><tbody>';
      for (const pr of data.properties) {
        h += `<tr>
          <td>${esc(pr.full_address || '')}</td>
          <td>${osintBadge(pr.property_type, 'amber')}</td>
          <td>${osintBadge(pr.relationship_type, 'cyan')}</td>
          <td>${fmtDate(pr.ownership_start)}</td>
          <td><button class="btn btn-sm btn-outline" onclick="openOsintProperty('${pr.id}')">Details</button></td>
        </tr>`;
      }
      h += '</tbody></table></div>';
    }

    // ── Vehicles ──
    if (data.vehicles.length) {
      h += '<div class="section-title">🚗 Vehicles</div>';
      h += '<div class="data-table"><table><thead><tr><th>Year</th><th>Make</th><th>Model</th><th>VIN</th><th>Plate</th></tr></thead><tbody>';
      for (const v of data.vehicles) {
        h += `<tr><td>${esc(v.year || '')}</td><td>${esc(v.make || '')}</td><td>${esc(v.model || '')}</td>
          <td style="font-family:monospace;font-size:.8rem">${esc(v.vin || '')}</td><td>${esc(v.license_plate || '')}</td></tr>`;
      }
      h += '</tbody></table></div>';
    }

    // ── Relationships ──
    if (data.relationships.length) {
      h += '<div class="section-title">🔗 Relationships</div>';
      h += '<div class="data-table"><table><thead><tr><th>Relationship</th><th>Related To</th><th>Strength</th><th>Confidence</th></tr></thead><tbody>';
      for (const r of data.relationships) {
        const relLabel = r.relationship || r.relationship_detail || '—';
        const nameDisplay = r.other_name
          ? `<a href="#" onclick="openOsintPerson('${r.other_id}');return false">${esc(r.other_name)}</a>`
          : esc((r.other_type || 'entity') + ': ' + (r.other_id || '').substring(0, 8) + '...');
        const strength = r.strength ? '●'.repeat(Math.min(r.strength, 5)) + ' (' + r.strength + ')' : '—';
        h += `<tr>
          <td>${osintBadge(relLabel, 'purple')}</td>
          <td>${nameDisplay}</td>
          <td>${strength}</td>
          <td>${osintBadge(r.confidence || '—', 'cyan')}</td></tr>`;
      }
      h += '</tbody></table></div>';
    }

    // ── Identifiers ──
    if (data.identifiers.length) {
      h += '<div class="section-title">🆔 Identifiers</div>';
      h += '<div class="data-table"><table><thead><tr><th>Type</th><th>Value</th><th>Issuer</th></tr></thead><tbody>';
      for (const id of data.identifiers) {
        h += `<tr><td>${osintBadge(id.identifier_type, 'blue')}</td>
          <td style="font-family:monospace">${esc(id.identifier_value || '')}</td>
          <td>${esc(id.issuing_authority || '')}</td></tr>`;
      }
      h += '</tbody></table></div>';
    }

    el.innerHTML = h;
  } catch (err) {
    el.innerHTML = `<div class="empty-state">❌ Failed: ${esc(err.message)}</div>`;
  }
}


// ═══════════════════════════════════════════════════════════
// COURT RECORDS BROWSER
// ═══════════════════════════════════════════════════════════

async function loadOsintCourtRecords() {
  osintCurrentView = 'court-records';
  const el = osintEl('records');

  const inv = $('osint-court-inv-filter') ? $('osint-court-inv-filter').value : '';
  const source = $('osint-court-source-filter') ? $('osint-court-source-filter').value : '';
  const q = $('osint-court-search') ? $('osint-court-search').value : '';

  el.innerHTML = '<div class="empty-state"><span class="spinner"></span></div>';

  let params = `?offset=${osintCourtOffset}&limit=50`;
  if (inv) params += `&investigation_id=${encodeURIComponent(inv)}`;
  if (source) params += `&source=${encodeURIComponent(source)}`;
  if (q) params += `&q=${encodeURIComponent(q)}`;

  try {
    const [data, invs, sources] = await Promise.all([
      api('/api/osint/court-records' + params),
      api('/api/osint/investigations'),
      api('/api/osint/sources'),
    ]);

    let h = '';

    // Filters
    h += '<div class="osint-filters">';
    h += `<select id="osint-court-inv-filter" onchange="osintCourtOffset=0;loadOsintCourtRecords()">
      <option value="">All Investigations</option>
      ${invs.map(i => `<option value="${i.id}" ${i.id===inv?'selected':''}>${esc(i.case_number)} — ${esc(i.title)}</option>`).join('')}
    </select>`;
    h += `<select id="osint-court-source-filter" onchange="osintCourtOffset=0;loadOsintCourtRecords()">
      <option value="">All Sources</option>
      ${sources.by_source.map(s => `<option value="${esc(s.source)}" ${s.source===source?'selected':''}>${esc(s.source)} (${s.cnt})</option>`).join('')}
    </select>`;
    h += `<input id="osint-court-search" placeholder="Search cases..." value="${esc(q)}"
      onkeydown="if(event.key==='Enter'){osintCourtOffset=0;loadOsintCourtRecords()}"/>`;
    h += `<span style="color:var(--muted);font-size:.85rem">${fmtNum(data.total)} cases</span>`;
    h += '</div>';

    // Table
    h += '<div class="data-table"><table><thead><tr>';
    h += '<th>Case #</th><th>Case Name</th><th>Court</th><th>Role</th><th>Filed</th><th>Records</th>';
    h += '</tr></thead><tbody>';
    for (const r of data.records) {
      const cnt = r.record_count || 1;
      h += `<tr onclick="openOsintCourtRecord('${r.id}')" style="cursor:pointer">
        <td style="font-family:monospace;font-size:.8rem">${esc(r.case_number || '')}</td>
        <td>${esc((r.case_name || '').substring(0, 80))}</td>
        <td style="font-size:.8rem">${esc((r.court_name || '').substring(0, 40))}</td>
        <td>${esc(r.role || '')}</td>
        <td style="white-space:nowrap">${fmtDate(r.filing_date)}</td>
        <td style="text-align:center">${cnt > 1 ? `<span style="background:rgba(59,130,246,.15);color:var(--cyan);padding:2px 8px;border-radius:10px;font-size:.8rem">${cnt}</span>` : '1'}</td>
      </tr>`;
    }
    h += '</tbody></table></div>';

    // Pagination
    h += '<div class="osint-pag">';
    if (osintCourtOffset > 0) {
      h += `<button class="btn btn-sm btn-outline" onclick="osintCourtOffset=Math.max(0,osintCourtOffset-50);loadOsintCourtRecords()">← Prev</button>`;
    }
    h += `<span style="color:var(--muted);font-size:.85rem">${osintCourtOffset+1}–${Math.min(osintCourtOffset+50,data.total)} of ${data.total}</span>`;
    if (osintCourtOffset + 50 < data.total) {
      h += `<button class="btn btn-sm btn-outline" onclick="osintCourtOffset+=50;loadOsintCourtRecords()">Next →</button>`;
    }
    h += '</div>';

    el.innerHTML = h;
  } catch (err) {
    el.innerHTML = `<div class="empty-state">❌ Failed: ${esc(err.message)}</div>`;
  }
}


async function openOsintCourtRecord(recId, fromContext) {
  osintCurrentView = 'record-detail';
  // Track where we came from for back navigation
  if (!fromContext) fromContext = osintCurrentPerson ? 'person' : 'records';
  const _backPersonId = osintCurrentPerson;

  // Render in the currently visible content container
  const el = fromContext === 'person'
    ? (osintEl('persons') || document.querySelector('main > section:not(.hidden) > div[id]'))
    : (osintEl('records') || document.querySelector('main > section:not(.hidden) > div[id]'));
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
      : `switchTab('osint-records')`;
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
      ${rd.api_url ? `<div style="margin-top:8px"><a href="https://www.courtlistener.com${esc(rd.api_url)}" target="_blank" style="color:var(--cyan);font-size:.85rem">🔗 View on CourtListener ↗</a></div>` : ''}
      ${rd.docket_id ? `<div style="margin-top:4px"><a href="https://www.courtlistener.com/docket/${rd.docket_id}/" target="_blank" style="color:var(--muted);font-size:.82rem">📋 View Full Docket ↗</a></div>` : ''}
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

    // Document Availability across sources
    if (data.document_availability && data.document_availability.length) {
      h += `<div class="section-title">📡 Document Availability (${data.document_availability.length} sources)</div>`;
      h += '<div class="data-table"><table><thead><tr><th>Source</th><th>Priority</th><th>Available</th><th>Free</th><th>Cost</th><th>Fetched</th><th>Actions</th></tr></thead><tbody>';
      for (const da of data.document_availability) {
        const availBadge = da.is_available
          ? '<span style="color:var(--green)">✓ Yes</span>'
          : '<span style="color:var(--red)">✗ No</span>';
        const freeBadge = da.is_free
          ? '<span style="color:var(--green)">Free</span>'
          : '<span style="color:var(--amber)">Paid</span>';
        const costStr = da.estimated_cost != null ? `$${da.estimated_cost.toFixed(2)}` : '—';
        const fetchedBadge = da.fetched
          ? '<span style="color:var(--green)">✓ Fetched</span>'
          : '<span style="color:var(--muted)">Not yet</span>';
        let actions = '';
        if (da.is_available && !da.fetched && da.document_url) {
          actions += `<a href="${esc(da.document_url)}" target="_blank" class="btn btn-sm btn-outline" style="font-size:.75rem">View</a> `;
        }
        if (da.is_available && !da.fetched) {
          const costNote = da.is_free ? '' : ` ($${(da.estimated_cost||0.10).toFixed(2)})`;
          actions += `<button class="btn btn-sm" style="font-size:.75rem;background:var(--cyan);color:#000;border:none;padding:2px 8px;border-radius:3px" onclick="markDocFetched('${da.id}')">Mark Fetched${costNote}</button>`;
        }
        h += `<tr>
          <td>${osintBadge(da.source, da.source === 'pacer' ? 'cyan' : da.source === 'courtlistener' ? 'blue' : 'purple')}</td>
          <td style="text-align:center;font-size:.8rem">${da.source_priority || '—'}</td>
          <td>${availBadge}</td>
          <td>${freeBadge}</td>
          <td style="font-family:monospace;font-size:.8rem">${costStr}</td>
          <td>${fetchedBadge}</td>
          <td>${actions || '—'}</td></tr>`;
      }
      h += '</tbody></table></div>';
    } else {
      // No availability data — show a note
      h += `<div class="section-title">📡 Document Availability</div>`;
      h += `<div style="padding:12px;color:var(--muted);font-size:.85rem">No source availability data recorded for this case.</div>`;
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
}


// ═══════════════════════════════════════════════════════════
// OSINT DOCUMENTS BROWSER
// ═══════════════════════════════════════════════════════════

async function loadOsintDocuments() {
  osintCurrentView = 'osint-docs';
  const el = osintEl('docs');

  const inv = $('osint-doc-inv-filter') ? $('osint-doc-inv-filter').value : '';
  const dtype = $('osint-doc-type-filter') ? $('osint-doc-type-filter').value : '';
  const q = $('osint-doc-search') ? $('osint-doc-search').value : '';

  el.innerHTML = '<div class="empty-state"><span class="spinner"></span></div>';

  let params = `?offset=${osintDocOffset}&limit=50`;
  if (inv) params += `&investigation_id=${encodeURIComponent(inv)}`;
  if (dtype) params += `&document_type=${encodeURIComponent(dtype)}`;
  if (q) params += `&q=${encodeURIComponent(q)}`;

  try {
    const [data, invs, sources] = await Promise.all([
      api('/api/osint/documents' + params),
      api('/api/osint/investigations'),
      api('/api/osint/sources'),
    ]);

    let h = '';
    h += '<div class="osint-filters">';
    h += `<select id="osint-doc-inv-filter" onchange="osintDocOffset=0;loadOsintDocuments()">
      <option value="">All Investigations</option>
      ${invs.map(i => `<option value="${i.id}" ${i.id===inv?'selected':''}>${esc(i.case_number)} — ${esc(i.title)}</option>`).join('')}
    </select>`;
    h += `<select id="osint-doc-type-filter" onchange="osintDocOffset=0;loadOsintDocuments()">
      <option value="">All Types</option>
      ${sources.by_doc_type.map(s => `<option value="${esc(s.doc_type)}" ${s.doc_type===dtype?'selected':''}>${esc(s.doc_type)} (${s.cnt})</option>`).join('')}
    </select>`;
    h += `<input id="osint-doc-search" placeholder="Search documents..." value="${esc(q)}"
      onkeydown="if(event.key==='Enter'){osintDocOffset=0;loadOsintDocuments()}"/>`;
    h += `<span style="color:var(--muted);font-size:.85rem">${fmtNum(data.total)} documents</span>`;
    h += '</div>';

    h += renderDocTable(data.documents);

    // Pagination
    h += '<div class="osint-pag">';
    if (osintDocOffset > 0) {
      h += `<button class="btn btn-sm btn-outline" onclick="osintDocOffset=Math.max(0,osintDocOffset-50);loadOsintDocuments()">← Prev</button>`;
    }
    h += `<span style="color:var(--muted);font-size:.85rem">${osintDocOffset+1}–${Math.min(osintDocOffset+50,data.total)} of ${data.total}</span>`;
    if (osintDocOffset + 50 < data.total) {
      h += `<button class="btn btn-sm btn-outline" onclick="osintDocOffset+=50;loadOsintDocuments()">Next →</button>`;
    }
    h += '</div>';

    el.innerHTML = h;
  } catch (err) {
    el.innerHTML = `<div class="empty-state">❌ Failed: ${esc(err.message)}</div>`;
  }
}


function renderDocTable(docs) {
  let h = '<div class="data-table"><table><thead><tr>';
  h += '<th>Title</th><th>Type</th><th>MIME</th><th>Size</th><th>Investigation</th><th>Created</th>';
  h += '</tr></thead><tbody>';
  for (const d of docs) {
    h += `<tr onclick="openOsintDocument('${d.id}')" style="cursor:pointer">
      <td>${esc((d.title || '').substring(0, 70))}</td>
      <td>${osintBadge(d.document_type, 'blue')}</td>
      <td style="font-size:.8rem">${esc(d.mime_type || '')}</td>
      <td style="white-space:nowrap">${fmtBytes(d.file_size_bytes)}</td>
      <td style="font-size:.8rem">${esc(d.inv_case_number || '')}</td>
      <td style="white-space:nowrap">${fmtDate(d.created_at)}</td>
    </tr>`;
  }
  h += '</tbody></table></div>';
  return h;
}

async function openOsintDocument(docId) {
  osintCurrentView = 'doc-detail';
  const el = document.querySelector('main > section:not(.hidden) > div[id]') || osintEl('docs');
  el.innerHTML = '<div class="empty-state"><span class="spinner"></span></div>';

  try {
    const d = await api(`/api/osint/documents/${docId}`);
    let h = '';

    h += `<div class="osint-breadcrumb">
      <a href="#" onclick="switchTab('osint-docs');return false">← Documents</a>
      <span> / </span><strong>${esc((d.title || '').substring(0, 60))}</strong>
    </div>`;

    h += `<div class="osint-detail-header"><div>
      <h2>${esc(d.title)}</h2>
      <div style="margin-top:6px;display:flex;gap:8px;flex-wrap:wrap">
        ${osintBadge(d.document_type, 'blue')}
        ${osintBadge(d.mime_type, 'cyan')}
        ${d.file_size_bytes ? osintBadge(fmtBytes(d.file_size_bytes), 'green') : ''}
        ${osintBadge(d.uploaded_by, 'purple')}
      </div>
    </div></div>`;

    h += '<div class="osint-fields">';
    h += osintField('File Path', `<span style="font-family:monospace;font-size:.8rem;word-break:break-all">${esc(d.file_path || '')}</span>`);
    h += osintField('Hash (SHA256)', `<span style="font-family:monospace;font-size:.75rem">${esc(d.file_hash_sha256 || '')}</span>`);
    h += osintField('Created', fmtDate(d.created_at));
    if (d.description) h += osintField('Description', esc(d.description));
    h += '</div>';

    // Content preview
    if (d.content_text) {
      h += '<div class="section-title">📝 Content</div>';
      h += `<div class="osint-content-preview">${esc(d.content_text)}</div>`;
    }
    if (d.ocr_text) {
      h += '<div class="section-title">🔍 OCR Text</div>';
      h += `<div class="osint-content-preview">${esc(d.ocr_text)}</div>`;
    }

    el.innerHTML = h;
  } catch (err) {
    el.innerHTML = `<div class="empty-state">❌ Failed: ${esc(err.message)}</div>`;
  }
}


// ═══════════════════════════════════════════════════════════
// PROPERTIES BROWSER
// ═══════════════════════════════════════════════════════════

async function loadOsintProperties() {
  osintCurrentView = 'properties';
  const el = osintEl('props');
  el.innerHTML = '<div class="empty-state"><span class="spinner"></span></div>';

  try {
    const props = await api('/api/osint/properties');
    let h = '<div class="section-title">🏠 Properties</div>';

    h += '<div class="data-table"><table><thead><tr>';
    h += '<th>Address</th><th>City</th><th>State</th><th>Type</th><th>Owners</th><th></th>';
    h += '</tr></thead><tbody>';
    for (const p of props) {
      h += `<tr>
        <td>${esc(p.full_address || '')}</td>
        <td>${esc(p.city || '')}</td>
        <td>${esc(p.state || '')}</td>
        <td>${osintBadge(p.property_type, 'amber')}</td>
        <td style="font-size:.85rem">${esc(p.owners || p.owner_name || '')}</td>
        <td><button class="btn btn-sm btn-outline" onclick="openOsintProperty('${p.id}')">Details</button></td>
      </tr>`;
    }
    h += '</tbody></table></div>';

    el.innerHTML = h;
  } catch (err) {
    el.innerHTML = `<div class="empty-state">❌ Failed: ${esc(err.message)}</div>`;
  }
}


async function openOsintProperty(propId) {
  osintCurrentView = 'property-detail';
  const el = document.querySelector('main > section:not(.hidden) > div[id]') || osintEl('props');
  el.innerHTML = '<div class="empty-state"><span class="spinner"></span></div>';

  try {
    const data = await api(`/api/osint/properties/${propId}`);
    const p = data.property;
    let h = '';

    h += `<div class="osint-breadcrumb">
      <a href="#" onclick="switchTab('osint-props');return false">← Properties</a>
      <span> / </span><strong>${esc(p.full_address || p.id)}</strong>
    </div>`;

    h += `<div class="osint-detail-header"><div>
      <h2>🏠 ${esc(p.full_address || 'Property')}</h2>
      <div style="margin-top:6px;display:flex;gap:8px;flex-wrap:wrap">
        ${osintBadge(p.property_type, 'amber')}
        ${p.city ? osintBadge(p.city + ', ' + (p.state || ''), 'blue') : ''}
        ${p.zip_code ? osintBadge(p.zip_code, 'cyan') : ''}
      </div>
    </div></div>`;

    h += '<div class="osint-fields">';
    h += osintField('APN / Parcel', p.apn || (p.raw_data && p.raw_data.apn));
    h += osintField('Year Built', p.year_built);
    h += osintField('Bedrooms', p.bedrooms);
    h += osintField('Bathrooms', p.bathrooms);
    h += osintField('Sq Ft', p.square_footage ? fmtNum(p.square_footage) : null);
    h += osintField('Lot Size', p.lot_size);
    h += '</div>';

    // Owners
    if (data.owners.length) {
      h += '<div class="section-title">👤 Owners</div>';
      h += '<div class="data-table"><table><thead><tr><th>Name</th><th>Relationship</th><th>Since</th><th>Until</th><th></th></tr></thead><tbody>';
      for (const o of data.owners) {
        h += `<tr><td>${esc(o.full_name)}</td><td>${osintBadge(o.relationship_type, 'cyan')}</td>
          <td>${fmtDate(o.ownership_start)}</td><td>${fmtDate(o.ownership_end)}</td>
          <td><button class="btn btn-sm btn-outline" onclick="openOsintPerson('${o.id}')">Profile</button></td></tr>`;
      }
      h += '</tbody></table></div>';
    }

    // Deeds
    if (data.deeds.length) {
      h += `<div class="section-title">📜 Deeds (${data.deeds.length})</div>`;
      h += '<div class="data-table"><table><thead><tr><th>Type</th><th>Grantor</th><th>Grantee</th><th>Amount</th><th>Recorded</th></tr></thead><tbody>';
      for (const d of data.deeds) {
        h += `<tr><td>${osintBadge(d.deed_type, 'blue')}</td>
          <td>${esc(d.grantor || '')}</td><td>${esc(d.grantee || '')}</td>
          <td>${d.sale_amount ? '$' + fmtNum(d.sale_amount) : '—'}</td>
          <td>${fmtDate(d.recording_date)}</td></tr>`;
      }
      h += '</tbody></table></div>';
    }

    // Valuations
    if (data.valuations.length) {
      h += `<div class="section-title">💰 Valuations (${data.valuations.length})</div>`;
      h += '<div class="data-table"><table><thead><tr><th>Year</th><th>Total</th><th>Land</th><th>Building</th><th>Estimated</th></tr></thead><tbody>';
      for (const v of data.valuations) {
        h += `<tr><td>${v.assessment_year || ''}</td>
          <td>${v.total_value ? '$'+fmtNum(v.total_value):'—'}</td>
          <td>${v.land_value ? '$'+fmtNum(v.land_value):'—'}</td>
          <td>${v.building_value ? '$'+fmtNum(v.building_value):'—'}</td>
          <td>${v.estimated_value ? '$'+fmtNum(v.estimated_value):'—'}</td></tr>`;
      }
      h += '</tbody></table></div>';
    }

    // Loans
    if (data.loans.length) {
      h += `<div class="section-title">🏦 Loans (${data.loans.length})</div>`;
      h += '<div class="data-table"><table><thead><tr><th>Lender</th><th>Amount</th><th>Type</th><th>Originated</th></tr></thead><tbody>';
      for (const l of data.loans) {
        h += `<tr><td>${esc(l.lender || '')}</td>
          <td>${l.loan_amount ? '$'+fmtNum(l.loan_amount):'—'}</td>
          <td>${osintBadge(l.loan_type, 'blue')}</td>
          <td>${fmtDate(l.origination_date)}</td></tr>`;
      }
      h += '</tbody></table></div>';
    }

    // Permits
    if (data.permits.length) {
      h += `<div class="section-title">🔨 Permits (${data.permits.length})</div>`;
      h += '<div class="data-table"><table><thead><tr><th>Number</th><th>Type</th><th>Description</th><th>Status</th><th>Issued</th></tr></thead><tbody>';
      for (const pm of data.permits) {
        h += `<tr><td>${esc(pm.permit_number || '')}</td><td>${osintBadge(pm.permit_type, 'blue')}</td>
          <td>${esc((pm.description || '').substring(0,60))}</td>
          <td>${osintBadge(pm.status, 'green')}</td><td>${fmtDate(pm.issue_date)}</td></tr>`;
      }
      h += '</tbody></table></div>';
    }

    // Residents
    if (data.residents.length) {
      h += `<div class="section-title">🏡 Residents (${data.residents.length})</div>`;
      h += '<div class="data-table"><table><thead><tr><th>Name</th><th>Move In</th><th>Move Out</th></tr></thead><tbody>';
      for (const r of data.residents) {
        h += `<tr><td>${esc(r.full_name || r.resident_name || '')}</td>
          <td>${fmtDate(r.move_in_date)}</td><td>${fmtDate(r.move_out_date)}</td></tr>`;
      }
      h += '</tbody></table></div>';
    }

    // Raw data
    if (p.raw_data && Object.keys(p.raw_data).length > 2) {
      h += '<div class="section-title">📦 Raw Data</div>';
      h += `<pre class="osint-raw">${syntaxHighlight(JSON.stringify(p.raw_data, null, 2))}</pre>`;
    }

    el.innerHTML = h;
  } catch (err) {
    el.innerHTML = `<div class="empty-state">❌ Failed: ${esc(err.message)}</div>`;
  }
}


// ═══════════════════════════════════════════════════════════
// PERSONS LIST
// ═══════════════════════════════════════════════════════════

async function loadOsintPersons() {
  osintCurrentView = 'persons';
  const el = osintEl('persons');
  el.innerHTML = '<div class="empty-state"><span class="spinner"></span></div>';

  const q = $('osint-person-search') ? $('osint-person-search').value : '';
  let params = q ? `?q=${encodeURIComponent(q)}` : '';

  try {
    const persons = await api('/api/osint/persons' + params);
    let h = '';

    h += '<div class="osint-filters">';
    h += `<input id="osint-person-search" placeholder="Search persons..." value="${esc(q)}"
      onkeydown="if(event.key==='Enter')loadOsintPersons()"/>`;
    h += `<span style="color:var(--muted);font-size:.85rem">${persons.length} persons</span>`;
    h += '</div>';

    h += '<div class="data-table"><table><thead><tr>';
    h += '<th>Name</th><th>DOB</th><th>Occupation</th><th>Court Records</th><th>Properties</th><th></th>';
    h += '</tr></thead><tbody>';
    for (const p of persons) {
      h += `<tr onclick="openOsintPerson('${p.id}')" style="cursor:pointer">
        <td><strong>${esc(p.full_name || (p.first_name + ' ' + p.last_name))}</strong>
          ${p.aliases && p.aliases.length ? `<br><span style="font-size:.75rem;color:var(--muted)">AKA: ${p.aliases.slice(0,3).map(a=>esc(a)).join(', ')}</span>` : ''}</td>
        <td style="white-space:nowrap">${fmtDate(p.date_of_birth)}</td>
        <td>${esc(p.occupation || '')}</td>
        <td class="blue"><strong>${fmtNum(p.court_count || 0)}</strong></td>
        <td class="amber"><strong>${fmtNum(p.property_count || 0)}</strong></td>
        <td><button class="btn btn-sm btn-outline" onclick="event.stopPropagation();openOsintPerson('${p.id}')">Profile</button></td>
      </tr>`;
    }
    h += '</tbody></table></div>';

    el.innerHTML = h;
  } catch (err) {
    el.innerHTML = `<div class="empty-state">❌ Failed: ${esc(err.message)}</div>`;
  }
}


// ═══════════════════════════════════════════════════════════
// OSINT GLOBAL SEARCH
// ═══════════════════════════════════════════════════════════

async function doOsintSearch() {
  const q = $('osint-search-input').value.trim();
  if (!q || q.length < 2) return;

  const el = $('osint-search-results');
  el.innerHTML = '<div class="empty-state"><span class="spinner"></span></div>';

  try {
    const data = await api('/api/osint/search', {
      method: 'POST',
      body: JSON.stringify({q})
    });

    let h = `<div style="color:var(--muted);margin-bottom:12px">${fmtNum(data.total)} results for "${esc(q)}"</div>`;

    if (data.persons.length) {
      h += '<div class="section-title">👤 Persons</div>';
      h += '<div class="data-table"><table><thead><tr><th>Name</th><th>DOB</th><th>Occupation</th><th></th></tr></thead><tbody>';
      for (const p of data.persons) {
        h += `<tr><td>${esc(p.full_name || p.first_name + ' ' + p.last_name)}</td>
          <td>${fmtDate(p.date_of_birth)}</td><td>${esc(p.occupation || '')}</td>
          <td><button class="btn btn-sm btn-outline" onclick="openOsintPerson('${p.id}')">View</button></td></tr>`;
      }
      h += '</tbody></table></div>';
    }

    if (data.court_records.length) {
      h += '<div class="section-title">⚖️ Court Records</div>';
      h += '<div class="data-table"><table><thead><tr><th>Case #</th><th>Case Name</th><th>Court</th><th>Filed</th><th></th></tr></thead><tbody>';
      for (const r of data.court_records) {
        h += `<tr><td style="font-family:monospace;font-size:.8rem">${esc(r.case_number || '')}</td>
          <td>${esc((r.case_name || '').substring(0,60))}</td>
          <td style="font-size:.85rem">${esc((r.court_name || '').substring(0,30))}</td>
          <td>${fmtDate(r.filing_date)}</td>
          <td><button class="btn btn-sm btn-outline" onclick="openOsintCourtRecord('${r.id}')">View</button></td></tr>`;
      }
      h += '</tbody></table></div>';
    }

    if (data.documents.length) {
      h += '<div class="section-title">📄 Documents</div>';
      h += renderDocTable(data.documents);
    }

    if (data.properties.length) {
      h += '<div class="section-title">🏠 Properties</div>';
      h += '<div class="data-table"><table><thead><tr><th>Address</th><th>City</th><th>Type</th><th></th></tr></thead><tbody>';
      for (const pr of data.properties) {
        h += `<tr><td>${esc(pr.full_address || '')}</td><td>${esc(pr.city || '')}</td>
          <td>${osintBadge(pr.property_type, 'amber')}</td>
          <td><button class="btn btn-sm btn-outline" onclick="openOsintProperty('${pr.id}')">View</button></td></tr>`;
      }
      h += '</tbody></table></div>';
    }

    if (!data.total) h = '<div class="empty-state">No results found</div>';

    el.innerHTML = h;
  } catch (err) {
    el.innerHTML = `<div class="empty-state">❌ Failed: ${esc(err.message)}</div>`;
  }
}

// Mark a document availability entry as fetched
async function markDocFetched(availId) {
  try {
    const resp = await fetch('/api/osint/doc-availability/update', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({id: availId, action: 'mark_fetched'})
    });
    if (!resp.ok) throw new Error('Failed');
    // Refresh current view
    const el = document.getElementById('osint-detail');
    if (el && el.dataset.currentRecordId) {
      openOsintCourtRecord(el.dataset.currentRecordId);
    } else {
      location.reload();
    }
  } catch(e) {
    alert('Error marking document as fetched: ' + e.message);
  }
}

