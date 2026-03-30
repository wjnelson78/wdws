"""Fix court records: consolidate by case_number in list view, show record count."""
import re

# ── Fix API: group court records by case_number ──
api_path = '/opt/wdws/dashboard/osint_api.py'
api = open(api_path).read()

# Replace the court records list query to use DISTINCT ON
old_query = '''    count_row = await p.fetchrow(f"SELECT count(*) AS total FROM court_records cr {where}", *params)

    rows = await p.fetch(f"""
        SELECT cr.*
        FROM court_records cr
        {where}
        ORDER BY cr.filing_date DESC NULLS LAST, cr.created_at DESC
        LIMIT {limit} OFFSET {offset}
    """, *params)

    return JSONResponse({
        "total": count_row["total"],
        "offset": offset,
        "limit": limit,
        "records": [_row(r) for r in rows],
    })'''

new_query = '''    count_row = await p.fetchrow(f"SELECT count(DISTINCT cr.case_number) AS total FROM court_records cr {where}", *params)

    rows = await p.fetch(f"""
        SELECT sub.*, rc.record_count FROM (
            SELECT DISTINCT ON (cr.case_number) cr.*
            FROM court_records cr
            {where}
            ORDER BY cr.case_number,
                     (cr.case_name IS NOT NULL AND cr.case_name != '') DESC,
                     cr.filing_date DESC NULLS LAST,
                     cr.created_at DESC
        ) sub
        LEFT JOIN (
            SELECT case_number, count(*) AS record_count
            FROM court_records cr {where}
            GROUP BY case_number
        ) rc ON rc.case_number = sub.case_number
        ORDER BY sub.filing_date DESC NULLS LAST, sub.created_at DESC
        LIMIT {limit} OFFSET {offset}
    """, *params + params)

    return JSONResponse({
        "total": count_row["total"],
        "offset": offset,
        "limit": limit,
        "records": [_row(r) for r in rows],
    })'''

if old_query in api:
    api = api.replace(old_query, new_query)
    print("API: Updated court records list query to deduplicate by case_number")
else:
    print("ERROR: could not find court records list query")
    # Try to find partial match for debugging
    if 'count(*) AS total FROM court_records' in api:
        print("  Found count query but full block doesn't match")
    if 'SELECT cr.*' in api:
        print("  Found SELECT cr.* but full block doesn't match")

open(api_path, 'w').write(api)

# ── Fix JS: show record count badge in court records table ──
js_path = '/opt/wdws/dashboard/osint_static/osint.js'
js = open(js_path).read()

# Update table header and row rendering
old_table = """    h += '<th>Case #</th><th>Case Name</th><th>Court</th><th>Type</th><th>Filed</th><th>Source</th>';"""
new_table = """    h += '<th>Case #</th><th>Case Name</th><th>Court</th><th>Role</th><th>Filed</th><th>Records</th>';"""

if old_table in js:
    js = js.replace(old_table, new_table)
    print("JS: Updated table headers")
else:
    print("ERROR: could not find table headers")

# Update table row rendering
old_row = """    for (const r of data.records) {
      h += `<tr onclick="openOsintCourtRecord('${r.id}')" style="cursor:pointer">
        <td style="font-family:monospace;font-size:.8rem">${esc(r.case_number || '')}</td>
        <td>${esc((r.case_name || '').substring(0, 70))}</td>
        <td style="font-size:.8rem">${esc((r.court_name || '').substring(0, 35))}</td>
        <td>${osintBadge(r.court_type || r.case_type, 'blue')}</td>
        <td style="white-space:nowrap">${fmtDate(r.filing_date)}</td>
        <td>${osintBadge(r.source || (r.raw_data && r.raw_data.source) || '', 'cyan')}</td>
      </tr>`;
    }"""

new_row = """    for (const r of data.records) {
      const cnt = r.record_count || 1;
      h += `<tr onclick="openOsintCourtRecord('${r.id}')" style="cursor:pointer">
        <td style="font-family:monospace;font-size:.8rem">${esc(r.case_number || '')}</td>
        <td>${esc((r.case_name || '').substring(0, 80))}</td>
        <td style="font-size:.8rem">${esc((r.court_name || '').substring(0, 40))}</td>
        <td>${esc(r.role || '')}</td>
        <td style="white-space:nowrap">${fmtDate(r.filing_date)}</td>
        <td style="text-align:center">${cnt > 1 ? `<span style="background:rgba(59,130,246,.15);color:var(--cyan);padding:2px 8px;border-radius:10px;font-size:.8rem">${cnt}</span>` : '1'}</td>
      </tr>`;
    }"""

if old_row in js:
    js = js.replace(old_row, new_row)
    print("JS: Updated row rendering with record count")
else:
    print("ERROR: could not find row rendering block")

# Update the total display
old_total = """    h += `<span style="color:var(--muted);font-size:.85rem">${fmtNum(data.total)} records</span>`;"""
new_total = """    h += `<span style="color:var(--muted);font-size:.85rem">${fmtNum(data.total)} cases</span>`;"""

if old_total in js:
    js = js.replace(old_total, new_total)
    print("JS: Updated total label to 'cases'")
else:
    print("WARN: could not find total label")

open(js_path, 'w').write(js)
print("Done.")
