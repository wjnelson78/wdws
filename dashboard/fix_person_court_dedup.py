"""Fix court records deduplication in PERSON DETAIL view (and investigation detail).
The main Court Records tab was already fixed, but the person profile page still
shows all raw records without consolidation."""

# ── Fix 1: Person detail API — deduplicate court records by case_number ──
api_path = '/opt/wdws/dashboard/osint_api.py'
api = open(api_path).read()

old_person_cr = """    # Court records
    court_records = await p.fetch(\"\"\"
        SELECT cr.*, pcr.role
        FROM court_records cr
        JOIN person_court_records pcr ON pcr.court_record_id = cr.id
        WHERE pcr.person_id = $1::uuid
        ORDER BY cr.filing_date DESC NULLS LAST
    \"\"\", person_id)"""

new_person_cr = """    # Court records (deduplicated by case_number, pick best row per case)
    court_records = await p.fetch(\"\"\"
        SELECT sub.*, rc.record_count FROM (
            SELECT DISTINCT ON (cr.case_number) cr.*, pcr.role
            FROM court_records cr
            JOIN person_court_records pcr ON pcr.court_record_id = cr.id
            WHERE pcr.person_id = $1::uuid
            ORDER BY cr.case_number,
                     (cr.case_name IS NOT NULL AND cr.case_name != '') DESC,
                     cr.filing_date DESC NULLS LAST,
                     cr.created_at DESC
        ) sub
        LEFT JOIN (
            SELECT cr.case_number, count(*) AS record_count
            FROM court_records cr
            JOIN person_court_records pcr ON pcr.court_record_id = cr.id
            WHERE pcr.person_id = $1::uuid
            GROUP BY cr.case_number
        ) rc ON rc.case_number = sub.case_number
        ORDER BY sub.filing_date DESC NULLS LAST
    \"\"\", person_id, person_id)"""

if old_person_cr in api:
    api = api.replace(old_person_cr, new_person_cr)
    print("API: Fixed person detail court records deduplication")
else:
    print("ERROR: Could not find person detail court records query")
    # Debug
    if 'pcr.role' in api:
        print("  Found pcr.role")
    if 'person_court_records pcr ON pcr.court_record_id' in api:
        print("  Found join clause")

open(api_path, 'w').write(api)

# ── Fix 2: JS person detail — update court records table ──
js_path = '/opt/wdws/dashboard/osint_static/osint.js'
js = open(js_path).read()

old_cr_section = """    // ── Court Records ──
    if (data.court_records.length) {
      h += `<div class="section-title">⚖️ Court Records (${data.court_records.length})</div>`;
      h += '<div class="data-table"><table><thead><tr><th>Case #</th><th>Case Name</th><th>Court</th><th>Role</th><th>Filed</th><th>Source</th></tr></thead><tbody>';
      for (const cr of data.court_records.slice(0, 100)) {
        h += `<tr onclick="openOsintCourtRecord('${cr.id}')" style="cursor:pointer">
          <td style="font-family:monospace;font-size:.8rem">${esc(cr.case_number || '')}</td>
          <td>${esc((cr.case_name || '').substring(0, 80))}</td>
          <td style="font-size:.8rem">${esc((cr.court_name || '').substring(0, 40))}</td>
          <td>${osintBadge(cr.role_in_case, 'purple')}</td>
          <td style="white-space:nowrap">${fmtDate(cr.filing_date)}</td>
          <td>${osintBadge(cr.source || (cr.raw_data && cr.raw_data.source) || '', 'cyan')}</td>
        </tr>`;
      }
      h += '</tbody></table></div>';
      if (data.court_records.length > 100) {
        h += `<div style="color:var(--muted);font-size:.8rem;margin-top:8px">Showing 100 of ${data.court_records.length} records</div>`;
      }
    }"""

new_cr_section = """    // ── Court Records (deduplicated) ──
    if (data.court_records.length) {
      h += `<div class="section-title">⚖️ Court Records (${data.court_records.length} cases)</div>`;
      h += '<div class="data-table"><table><thead><tr><th>Case #</th><th>Case Name</th><th>Court</th><th>Role</th><th>Filed</th><th>Recs</th></tr></thead><tbody>';
      for (const cr of data.court_records.slice(0, 100)) {
        const cnt = cr.record_count || 1;
        h += `<tr onclick="openOsintCourtRecord('${cr.id}')" style="cursor:pointer">
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
    }"""

if old_cr_section in js:
    js = js.replace(old_cr_section, new_cr_section)
    print("JS: Fixed person detail court records table")
else:
    print("ERROR: Could not find person detail court records section in JS")
    # Debug: check each line
    lines = old_cr_section.split('\n')
    for i, line in enumerate(lines[:5]):
        stripped = line.strip()
        if stripped and stripped in js:
            print(f"  Line {i} found: {stripped[:60]}")
        elif stripped:
            print(f"  Line {i} NOT FOUND: {stripped[:60]}")

open(js_path, 'w').write(js)
print("Done.")
