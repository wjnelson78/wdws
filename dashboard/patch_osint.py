#!/usr/bin/env python3
"""
Patch script: Adds OSINT Investigation Database module to Athena Dashboard.
Run on the server: python3 /opt/wdws/dashboard/patch_osint.py
"""

import re

APP_PY = "/opt/wdws/dashboard/app.py"

with open(APP_PY, "r") as f:
    content = f.read()

# ── BACKUP ──
with open(APP_PY + ".pre_osint", "w") as f:
    f.write(content)
print("✅ Backup saved to app.py.pre_osint")

changes = 0

# ═══════════════════════════════════════════════════════════
# 1. Add import for osint_api module after existing imports
# ═══════════════════════════════════════════════════════════
IMPORT_MARKER = "import uvicorn"
IMPORT_ADDITION = """import uvicorn

# OSINT Investigation Database
import sys
sys.path.insert(0, '/opt/wdws/dashboard')
from osint_api import get_osint_routes, get_osint_pool"""

if "from osint_api import" not in content:
    content = content.replace(IMPORT_MARKER, IMPORT_ADDITION, 1)
    changes += 1
    print("✅ Added osint_api import")
else:
    print("⏩ osint_api import already present")

# ═══════════════════════════════════════════════════════════
# 2. Add OSINT auth-wrapping + routes before the Static files mount
# ═══════════════════════════════════════════════════════════
ROUTE_MARKER = '    # Static files\n    Mount("/static", StaticFiles(directory="/opt/wdws/dashboard/static"), name="static"),'

ROUTE_ADDITION = """    # ── OSINT Investigation Database ──
    *[Route(r.path, _osint_auth_wrap(r.endpoint), methods=r.methods) for r in get_osint_routes()],
    # Static files — dashboard assets
    Mount("/static", StaticFiles(directory="/opt/wdws/dashboard/static"), name="static"),
    # Static files — OSINT module assets
    Mount("/osint-static", StaticFiles(directory="/opt/wdws/dashboard/osint_static"), name="osint-static"),"""

if "/api/osint/" not in content:
    content = content.replace(ROUTE_MARKER, ROUTE_ADDITION, 1)
    changes += 1
    print("✅ Added OSINT routes")
else:
    print("⏩ OSINT routes already present")

# ═══════════════════════════════════════════════════════════
# 3. Add the _osint_auth_wrap helper before the routes list
# ═══════════════════════════════════════════════════════════
AUTH_MARKER = """# ══════════════════════════════════════════════════════════════
#  ROUTES + MIDDLEWARE
# ══════════════════════════════════════════════════════════════"""

AUTH_WRAP = """# ── OSINT auth wrapper ──
def _osint_auth_wrap(endpoint):
    \"\"\"Wrap OSINT endpoints with dashboard auth check.\"\"\"
    async def wrapped(request):
        err = await _check_auth(request)
        if err:
            return err
        return await endpoint(request)
    wrapped.__name__ = getattr(endpoint, '__name__', 'osint')
    return wrapped


# ══════════════════════════════════════════════════════════════
#  ROUTES + MIDDLEWARE
# ══════════════════════════════════════════════════════════════"""

if "_osint_auth_wrap" not in content:
    content = content.replace(AUTH_MARKER, AUTH_WRAP, 1)
    changes += 1
    print("✅ Added _osint_auth_wrap helper")
else:
    print("⏩ _osint_auth_wrap already present")

# ═══════════════════════════════════════════════════════════
# 4. Add "Investigations" nav category to the navigation
# ═══════════════════════════════════════════════════════════
NAV_MARKER = """  <div class="nav-category" data-category="content">
    <button class="nav-category-btn">📚 Data & Content <span class="arrow">▼</span></button>"""

NAV_ADDITION = """  <div class="nav-category" data-category="osint">
    <button class="nav-category-btn">🔍 Investigations <span class="arrow">▼</span></button>
    <div class="nav-dropdown">
      <button class="nav-dropdown-item" data-tab="osint-inv" onclick="switchTab('osint-inv')">📂 Investigations</button>
      <button class="nav-dropdown-item" data-tab="osint-persons" onclick="switchTab('osint-persons')">👤 Persons</button>
      <button class="nav-dropdown-item" data-tab="osint-records" onclick="switchTab('osint-records')">⚖️ Court Records</button>
      <button class="nav-dropdown-item" data-tab="osint-docs" onclick="switchTab('osint-docs')">📄 OSINT Documents</button>
      <button class="nav-dropdown-item" data-tab="osint-props" onclick="switchTab('osint-props')">🏠 Properties</button>
      <button class="nav-dropdown-item" data-tab="osint-search" onclick="switchTab('osint-search')">🔎 OSINT Search</button>
    </div>
  </div>
  <div class="nav-category" data-category="content">
    <button class="nav-category-btn">📚 Data & Content <span class="arrow">▼</span></button>"""

if 'data-category="osint"' not in content:
    content = content.replace(NAV_MARKER, NAV_ADDITION, 1)
    changes += 1
    print("✅ Added Investigations nav category")
else:
    print("⏩ Investigations nav already present")

# ═══════════════════════════════════════════════════════════
# 5. Add OSINT tab sections before </main>
# ═══════════════════════════════════════════════════════════
MAIN_END = "\n</main>"

OSINT_TABS = """
<!-- ═══ OSINT: INVESTIGATIONS TAB ═══ -->
<section id="tab-osint-inv" class="hidden">
  <div id="osint-content"><div class="empty-state"><span class="spinner"></span></div></div>
</section>

<!-- ═══ OSINT: PERSONS TAB ═══ -->
<section id="tab-osint-persons" class="hidden">
  <div id="osint-content"><div class="empty-state"><span class="spinner"></span></div></div>
</section>

<!-- ═══ OSINT: COURT RECORDS TAB ═══ -->
<section id="tab-osint-records" class="hidden">
  <div id="osint-content"><div class="empty-state"><span class="spinner"></span></div></div>
</section>

<!-- ═══ OSINT: DOCUMENTS TAB ═══ -->
<section id="tab-osint-docs" class="hidden">
  <div id="osint-content"><div class="empty-state"><span class="spinner"></span></div></div>
</section>

<!-- ═══ OSINT: PROPERTIES TAB ═══ -->
<section id="tab-osint-props" class="hidden">
  <div id="osint-content"><div class="empty-state"><span class="spinner"></span></div></div>
</section>

<!-- ═══ OSINT: SEARCH TAB ═══ -->
<section id="tab-osint-search" class="hidden">
  <div style="display:flex;gap:12px;align-items:center;margin-bottom:16px">
    <input id="osint-search-input" placeholder="Search across all OSINT data..." style="flex:1;padding:10px 14px;font-size:1rem"
      onkeydown="if(event.key==='Enter')doOsintSearch()"/>
    <button class="btn btn-primary" onclick="doOsintSearch()">🔍 Search</button>
  </div>
  <div id="osint-search-results"></div>
</section>

</main>"""

if 'tab-osint-inv' not in content:
    content = content.replace(MAIN_END, OSINT_TABS, 1)
    changes += 1
    print("✅ Added OSINT tab sections")
else:
    print("⏩ OSINT tab sections already present")

# ═══════════════════════════════════════════════════════════
# 6. Add OSINT tab loaders to the switchTab loaders map
# ═══════════════════════════════════════════════════════════
LOADER_MARKER = "    chatroom: loadChatRoom,\n  };"

LOADER_ADDITION = """    chatroom: loadChatRoom,
    'osint-inv': loadOsintInvestigations,
    'osint-persons': loadOsintPersons,
    'osint-records': loadOsintCourtRecords,
    'osint-docs': loadOsintDocuments,
    'osint-props': loadOsintProperties,
    'osint-search': ()=>{},
  };"""

if "'osint-inv'" not in content:
    content = content.replace(LOADER_MARKER, LOADER_ADDITION, 1)
    changes += 1
    print("✅ Added OSINT tab loaders to switchTab")
else:
    print("⏩ OSINT tab loaders already present")

# ═══════════════════════════════════════════════════════════
# 7. Add <script src> for osint.js before closing </script>
#    and add OSINT CSS
# ═══════════════════════════════════════════════════════════
# Find the last </script> and add the external script before </body>
BODY_END = "</body></html>"
SCRIPT_ADDITION = """<script src="/osint-static/osint.js"></script>
</body></html>"""

if 'osint.js' not in content:
    content = content.replace(BODY_END, SCRIPT_ADDITION, 1)
    changes += 1
    print("✅ Added osint.js script tag")
else:
    print("⏩ osint.js already included")

# ═══════════════════════════════════════════════════════════
# 8. Add OSINT-specific CSS
# ═══════════════════════════════════════════════════════════
CSS_MARKER = "main{flex:1;padding:24px;max-width:1500px;margin:0 auto;width:100%}"

CSS_ADDITION = """main{flex:1;padding:24px;max-width:1500px;margin:0 auto;width:100%}

/* OSINT Investigation Module */
.osint-inv-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(360px,1fr));gap:16px}
.osint-inv-card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:20px;cursor:pointer;transition:all .2s}
.osint-inv-card:hover{border-color:var(--accent);box-shadow:0 4px 16px rgba(59,130,246,.12)}
.osint-inv-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:8px}
.osint-inv-case{font-family:monospace;font-size:.85rem;color:var(--cyan)}
.osint-inv-title{font-size:1.05rem;font-weight:600;margin-bottom:6px}
.osint-inv-desc{font-size:.85rem;color:var(--muted);margin-bottom:8px;line-height:1.4}
.osint-inv-stats{display:flex;gap:14px;font-size:.8rem;color:var(--muted);flex-wrap:wrap}
.osint-inv-date{font-size:.75rem;color:var(--muted);margin-top:8px}
.osint-breadcrumb{padding:10px 0;margin-bottom:16px;font-size:.9rem;color:var(--muted);border-bottom:1px solid var(--border)}
.osint-breadcrumb a{color:var(--cyan);text-decoration:none}
.osint-breadcrumb strong{color:var(--text)}
.osint-detail-header{padding:16px 0;border-bottom:1px solid var(--border);margin-bottom:16px}
.osint-detail-header h2{font-size:1.3rem;font-weight:700}
.osint-fields{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:12px;padding:16px 0}
.osint-person-header{display:flex;gap:20px;align-items:center;padding:20px;background:var(--surface);border:1px solid var(--border);border-radius:var(--radius)}
.osint-person-avatar{font-size:3rem;width:64px;height:64px;display:flex;align-items:center;justify-content:center;background:var(--surface2);border-radius:50%}
.osint-person-header h2{font-size:1.3rem;font-weight:700}
.osint-contact-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(250px,1fr));gap:14px;padding:8px 0}
.osint-contact-section{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:14px}
.osint-contact-section h4{font-size:.85rem;margin-bottom:8px;color:var(--cyan)}
.osint-contact-item{padding:6px 0;font-size:.9rem;border-bottom:1px solid var(--border)}
.osint-contact-item:last-child{border-bottom:none}
.osint-filters{display:flex;gap:10px;align-items:center;margin-bottom:16px;flex-wrap:wrap;padding:12px;background:var(--surface);border:1px solid var(--border);border-radius:var(--radius)}
.osint-filters input{min-width:180px}
.osint-pag{display:flex;gap:12px;align-items:center;justify-content:center;padding:16px 0}
.osint-timeline{border-left:3px solid var(--border);padding-left:20px;margin-left:12px}
.osint-timeline-item{position:relative;padding-bottom:16px}
.osint-timeline-item::before{content:'';position:absolute;left:-27px;top:4px;width:12px;height:12px;border-radius:50%;background:var(--accent);border:2px solid var(--bg)}
.osint-timeline-date{font-size:.8rem;color:var(--muted);margin-bottom:4px}
.osint-timeline-content{font-size:.9rem}
.osint-timeline-content strong{color:var(--cyan);margin-right:8px}
.osint-note{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:14px;margin-bottom:10px}
.osint-note-header{font-size:.8rem;color:var(--muted);margin-bottom:6px}
.osint-note-body{font-size:.9rem;line-height:1.5;white-space:pre-wrap}
.osint-raw{background:var(--bg);border:1px solid var(--border);border-radius:var(--radius);padding:16px;font-family:monospace;font-size:.8rem;overflow-x:auto;max-height:400px;overflow-y:auto;white-space:pre;line-height:1.6}
.osint-content-preview{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:16px;font-size:.85rem;line-height:1.6;max-height:500px;overflow-y:auto;white-space:pre-wrap;font-family:monospace}
.cols-2{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:16px}"""

if ".osint-inv-grid" not in content:
    content = content.replace(CSS_MARKER, CSS_ADDITION, 1)
    changes += 1
    print("✅ Added OSINT CSS")
else:
    print("⏩ OSINT CSS already present")

# ═══════════════════════════════════════════════════════════
# 9. Fix: Multiple osint-content divs have the same ID.
#    Each tab needs its own content div. The JS uses $('osint-content')
#    so we need to ensure the active tab's section has this ID.
#    Solution: give each tab a unique container and use the
#    JS to target the active one via the parent section.
# ═══════════════════════════════════════════════════════════
# Actually, the simpler fix: all OSINT tabs share one section with
# a single content div, and we use sub-navigation within it.
# Let me restructure the tab sections.

# Remove the per-tab sections and use a single unified section
OSINT_TABS_V2 = """
<!-- ═══ OSINT MODULE TABS ═══ -->
<section id="tab-osint-inv" class="hidden">
  <div id="osint-content"><div class="empty-state"><span class="spinner"></span></div></div>
</section>
<section id="tab-osint-persons" class="hidden">
  <div id="osint-persons-content"><div class="empty-state"><span class="spinner"></span></div></div>
</section>
<section id="tab-osint-records" class="hidden">
  <div id="osint-records-content"><div class="empty-state"><span class="spinner"></span></div></div>
</section>
<section id="tab-osint-docs" class="hidden">
  <div id="osint-docs-content"><div class="empty-state"><span class="spinner"></span></div></div>
</section>
<section id="tab-osint-props" class="hidden">
  <div id="osint-props-content"><div class="empty-state"><span class="spinner"></span></div></div>
</section>
<section id="tab-osint-search" class="hidden">
  <div style="display:flex;gap:12px;align-items:center;margin-bottom:16px">
    <input id="osint-search-input" placeholder="Search across all OSINT data..." style="flex:1;padding:10px 14px;font-size:1rem"
      onkeydown="if(event.key==='Enter')doOsintSearch()"/>
    <button class="btn btn-primary" onclick="doOsintSearch()">🔍 Search</button>
  </div>
  <div id="osint-search-results"></div>
</section>

</main>"""

# Replace the v1 tabs we just added with v2 (unique IDs per tab content)
if 'osint-persons-content' not in content and 'tab-osint-inv' in content:
    # Find and replace the OSINT tabs section
    pattern = r'<!-- ═══ OSINT.*?</section>\s*\n</main>'
    match = re.search(pattern, content, re.DOTALL)
    if match:
        content = content[:match.start()] + OSINT_TABS_V2 + content[match.end():]
        changes += 1
        print("✅ Fixed OSINT tab sections with unique IDs")
    else:
        print("⚠️  Could not find OSINT tabs to fix IDs")
else:
    print("⏩ OSINT tab IDs already unique")

# ═══════════════════════════════════════════════════════════
# Write patched file
# ═══════════════════════════════════════════════════════════
with open(APP_PY, "w") as f:
    f.write(content)

print(f"\n🎯 Applied {changes} patches to app.py")
print("📌 Restart the dashboard service to apply: systemctl restart athena-dashboard")
