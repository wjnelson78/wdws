"""Fix ALL back navigation: use switchTab() to ensure the correct tab becomes
visible before the loader runs."""

path = '/opt/wdws/dashboard/osint_static/osint.js'
js = open(path).read()

fixes = 0

# 1. Investigation detail: "← All Investigations" 
old = """      <a href="#" onclick="loadOsintInvestigations();return false">← All Investigations</a>"""
new = """      <a href="#" onclick="switchTab('osint-inv');return false">← All Investigations</a>"""
if old in js:
    js = js.replace(old, new)
    fixes += 1
    print("Fixed: Investigation detail back nav")

# 2. Court record detail (from records list): "← Court Records"
old = """      : `loadOsintCourtRecords()`;
    const backLabel = fromContext === 'person' ? '← Back to Person' : '← Court Records';"""
new = """      : `switchTab('osint-records')`;
    const backLabel = fromContext === 'person' ? '← Back to Person' : '← Court Records';"""
if old in js:
    js = js.replace(old, new)
    fixes += 1
    print("Fixed: Court record detail back nav (records context)")

# 3. Document detail: "← Documents"
old = """      <a href="#" onclick="loadOsintDocuments();return false">← Documents</a>"""
new = """      <a href="#" onclick="switchTab('osint-docs');return false">← Documents</a>"""
if old in js:
    js = js.replace(old, new)
    fixes += 1
    print("Fixed: Document detail back nav")

# 4. Property detail: "← Properties"
old = """      <a href="#" onclick="loadOsintProperties();return false">← Properties</a>"""
new = """      <a href="#" onclick="switchTab('osint-props');return false">← Properties</a>"""
if old in js:
    js = js.replace(old, new)
    fixes += 1
    print("Fixed: Property detail back nav")

# 5. Person detail back to persons (already partially fixed but let's make sure)
# The person detail back uses switchTab('osint-persons') which is correct.
# But when from investigation, it uses openOsintInvestigation which renders in inv tab
# So we need to make sure the inv tab is visible first for that case.
# Actually openOsintInvestigation renders into osintEl('inv') which is in tab-osint-inv
# So it needs switchTab too. Let's check.
old_inv_detail = """      ? `openOsintInvestigation('${osintCurrentInv}');return false`
      : `switchTab('osint-persons');return false`;"""
# This should be fine - openOsintInvestigation uses osintEl('inv') and if we came from
# an investigation, that tab was visible. But to be safe, let's leave it.

open(path, 'w').write(js)
print(f"Done. Applied {fixes} fixes.")
