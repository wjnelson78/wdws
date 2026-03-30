"""Fix back navigation for ALL detail views.
The problem: detail views render into whichever container is visible,
but back buttons call functions that render into SPECIFIC containers
(e.g. loadOsintInvestigations renders into osint-content, not osint-persons-content).

Fix approach: 
1. openOsintPerson: track where it was opened from, back goes to right list
2. openOsintCourtRecord: already fixed (fromContext)  
3. All back navigation: switch to correct tab first, then load
"""

path = '/opt/wdws/dashboard/osint_static/osint.js'
js = open(path).read()

# Fix openOsintPerson: accept fromContext, fix breadcrumb back navigation
old_person_fn_start = """async function openOsintPerson(personId) {
  osintCurrentPerson = personId;
  osintCurrentView = 'person-detail';
  // Person detail can be opened from multiple tabs — render into whatever is visible
  const el = document.querySelector('main > section:not(.hidden) > div[id]') || osintEl('persons');
  el.innerHTML = '<div class="empty-state"><span class="spinner"></span></div>';

  try {
    const data = await api(`/api/osint/persons/${personId}`);
    const p = data.person;
    let h = '';

    // Breadcrumb
    h += `<div class="osint-breadcrumb">
      <a href="#" onclick="loadOsintInvestigations();return false">← Investigations</a>`;
    if (osintCurrentInv) {
      h += ` <span>/</span> <a href="#" onclick="openOsintInvestigation('${osintCurrentInv}');return false">Investigation</a>`;
    }
    h += ` <span>/</span> <strong>${esc(p.full_name || p.first_name + ' ' + p.last_name)}</strong></div>`;"""

new_person_fn_start = """async function openOsintPerson(personId) {
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
    h += ` <span>/</span> <strong>${esc(p.full_name || p.first_name + ' ' + p.last_name)}</strong></div>`;"""

if old_person_fn_start in js:
    js = js.replace(old_person_fn_start, new_person_fn_start)
    print("JS: Fixed person detail back navigation")
else:
    print("ERROR: Could not find person detail breadcrumb block")
    # Debug matching
    lines = old_person_fn_start.split('\n')
    for i, line in enumerate(lines[:5]):
        s = line.rstrip()
        if s and s in js:
            pass  # found
        elif s:
            print(f"  Line {i} NOT FOUND: {repr(s[:80])}")

open(path, 'w').write(js)
print("Done.")
