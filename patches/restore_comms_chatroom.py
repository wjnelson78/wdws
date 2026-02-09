#!/usr/bin/env python3
"""
Restore missing Agent Comms + Chat Room tabs to dashboard.
Extracts code from .bak_comms backup and inserts into current app.py.
"""
import re, shutil

APP = '/opt/wdws/dashboard/app.py'
BACKUP = '/opt/wdws/dashboard/app.py.bak_comms'

# Read both files
with open(APP) as f:
    lines = f.readlines()
with open(BACKUP) as f:
    bak = f.readlines()

# Safety backup
shutil.copy2(APP, APP + '.pre_restore')

# ─────────────────────────────────────────────────────────
# 1. NAV BUTTONS: Insert comms + chatroom after security
# ─────────────────────────────────────────────────────────
nav_comms = '  <button data-tab="comms" onclick="switchTab(\'comms\')">📡 Agent Comms</button>\n'
nav_chatroom = '  <button data-tab="chatroom" onclick="switchTab(\'chatroom\')">💬 Chat Room</button>\n'

inserted_nav = False
for i, line in enumerate(lines):
    if 'data-tab="security"' in line:
        lines.insert(i + 1, nav_chatroom)
        lines.insert(i + 1, nav_comms)
        inserted_nav = True
        break
if not inserted_nav:
    print("ERROR: Could not find security nav button")
    exit(1)
print(f"✅ Inserted comms + chatroom nav buttons after security (line {i+1})")

# ─────────────────────────────────────────────────────────
# 2. TAB SECTIONS: Insert comms + chatroom sections before </main>
# ─────────────────────────────────────────────────────────
# Extract from backup: lines 3257 to 3410 (tab-comms + tab-chatroom sections)
# bak_comms has tab-comms at 3257 and tab-chatroom at 3289, ending before </main>
comms_start = None
comms_end = None
for idx, bline in enumerate(bak):
    if '<section id="tab-comms"' in bline:
        comms_start = idx
    if comms_start is not None and '</section>' in bline and 'tab-chatroom' not in bline:
        # Find the chatroom section end
        pass
    if comms_start is not None and bline.strip() == '</main>':
        comms_end = idx
        break

if comms_start is None or comms_end is None:
    print("ERROR: Could not find comms/chatroom sections in backup")
    exit(1)

tab_sections = bak[comms_start:comms_end]
print(f"✅ Extracted {len(tab_sections)} lines of comms+chatroom HTML from backup (lines {comms_start+1}-{comms_end})")

# Find </main> in current file and insert before it
main_inserted = False
for i, line in enumerate(lines):
    if line.strip() == '</main>':
        for j, sec_line in enumerate(tab_sections):
            lines.insert(i + j, sec_line)
        main_inserted = True
        break
if not main_inserted:
    print("ERROR: Could not find </main> in current file")
    exit(1)
print(f"✅ Inserted comms+chatroom tab sections before </main>")

# ─────────────────────────────────────────────────────────
# 3. ROUTES: Add chatroom + comms routes
# ─────────────────────────────────────────────────────────
route_lines = [
    '    Route("/api/agents/comms", api_agent_comms),\n',
    '    Route("/api/agents/chatroom/channels", api_chatroom_channels),\n',
    '    Route("/api/agents/chatroom/messages", api_chatroom_messages),\n',
    '    Route("/api/agents/chatroom/post", api_chatroom_post, methods=["POST"]),\n',
    '    Route("/api/agents/chatroom/stats", api_chatroom_stats),\n',
]

# Find the agents route block and add after the last agent route
routes_inserted = False
for i, line in enumerate(lines):
    if 'Route("/api/agents/' in line and '/detail"' in line:
        # Insert after this line
        for j, rline in enumerate(route_lines):
            lines.insert(i + 1 + j, rline)
        routes_inserted = True
        break
if not routes_inserted:
    print("ERROR: Could not find agent routes block")
    exit(1)
print(f"✅ Inserted {len(route_lines)} route definitions")

# ─────────────────────────────────────────────────────────
# 4. API ENDPOINTS: Add Python functions
# ─────────────────────────────────────────────────────────
# Extract api_agent_comms (770 to 1011 in backup)
api_comms_start = None
api_comms_end = None
for idx, bline in enumerate(bak):
    if bline.startswith('async def api_agent_comms('):
        api_comms_start = idx
    if api_comms_start is not None and idx > api_comms_start and bline.startswith('async def '):
        api_comms_end = idx
        break

api_comms_code = bak[api_comms_start:api_comms_end]
print(f"✅ Extracted api_agent_comms: {len(api_comms_code)} lines")

# Extract chatroom APIs (2008 to 2159 in backup)
chatroom_api_start = None
chatroom_api_end = None
for idx, bline in enumerate(bak):
    if bline.startswith('async def api_chatroom_channels('):
        chatroom_api_start = idx
    if chatroom_api_start is not None and bline.startswith('async def api_chat_conversations('):
        chatroom_api_end = idx
        break

chatroom_api_code = bak[chatroom_api_start:chatroom_api_end]
print(f"✅ Extracted chatroom APIs: {len(chatroom_api_code)} lines")

# Insert api_agent_comms before api_agent_detail
api_inserted_1 = False
for i, line in enumerate(lines):
    if line.startswith('async def api_agent_detail('):
        for j, aline in enumerate(api_comms_code):
            lines.insert(i + j, aline)
        api_inserted_1 = True
        break
if not api_inserted_1:
    print("ERROR: Could not find api_agent_detail to insert comms API before")
    exit(1)
print(f"✅ Inserted api_agent_comms function ({len(api_comms_code)} lines)")

# Insert chatroom APIs before api_chat_conversations
api_inserted_2 = False
for i, line in enumerate(lines):
    if line.startswith('async def api_chat_conversations('):
        for j, aline in enumerate(chatroom_api_code):
            lines.insert(i + j, aline)
        api_inserted_2 = True
        break
if not api_inserted_2:
    print("ERROR: Could not find api_chat_conversations to insert chatroom APIs before")
    exit(1)
print(f"✅ Inserted chatroom API functions ({len(chatroom_api_code)} lines)")

# ─────────────────────────────────────────────────────────
# 5. switchTab: Add comms + chatroom loaders
# ─────────────────────────────────────────────────────────
switchtab_fixed = False
text = ''.join(lines)
# Find the loaders object in switchTab and add comms + chatroom
old_security = "security: loadACL,"
new_security = "security: loadACL,\n    comms: loadComms,\n    chatroom: ()=>{ crLoadChannels(); crLoadMessages(); crStartPoll(); },"
if old_security in text:
    text = text.replace(old_security, new_security, 1)
    switchtab_fixed = True
    print("✅ Added comms + chatroom to switchTab loaders")
else:
    print("WARNING: Could not find switchTab loaders to patch")

# ─────────────────────────────────────────────────────────
# 6. JS FUNCTIONS: Add loadComms + chatroom JS
# ─────────────────────────────────────────────────────────
# Extract loadComms and rendering functions (5184 to 5393 in backup)
js_comms_start = None
js_comms_end = None
for idx, bline in enumerate(bak):
    if 'async function loadComms()' in bline:
        js_comms_start = idx
    if js_comms_start is not None and 'function crAgentClass(' in bline:
        js_comms_end = idx
        break

js_comms_code = ''.join(bak[js_comms_start:js_comms_end])
print(f"✅ Extracted comms JS: {js_comms_end - js_comms_start} lines")

# Extract chatroom JS functions (5394 to end of script in backup)
js_cr_start = None
js_cr_end = None
for idx, bline in enumerate(bak):
    if 'function crAgentClass(' in bline:
        js_cr_start = idx
    if js_cr_start is not None and '</script>' in bline:
        js_cr_end = idx
        break

js_cr_code = ''.join(bak[js_cr_start:js_cr_end])
print(f"✅ Extracted chatroom JS: {js_cr_end - js_cr_start} lines")

# Also need the chatroom state variables
cr_state_vars = """
// ═══ CHATROOM STATE ═══
let crChannels=[], crMessages=[], crCurrentChannel='general', crReplyTo=null, crPollTimer=null, crOldestId=null;
let crChannelDescriptions = {
  general: 'Team-wide conversations',
  alerts: 'System alerts and warnings',
  security: 'Security events and incidents',
  legal: 'Legal case discussions',
  medical: 'Medical records and health data',
  ops: 'Operations and infrastructure',
  'code-fixes': 'Code changes and patches'
};
"""

# Insert all JS before </script>
js_all = '\n' + cr_state_vars + '\n' + js_comms_code + '\n' + js_cr_code + '\n'

# Find </script> and insert before it
if '</script>' in text:
    text = text.replace('</script>', js_all + '</script>', 1)
    print("✅ Inserted all JS functions before </script>")
else:
    print("ERROR: Could not find </script>")
    exit(1)

# ─────────────────────────────────────────────────────────
# 7. Write result
# ─────────────────────────────────────────────────────────
with open(APP, 'w') as f:
    f.write(text)

final_lines = text.count('\n')
print(f"\n✅ Restoration complete! File now has {final_lines} lines")
print(f"   Backup saved to {APP}.pre_restore")
