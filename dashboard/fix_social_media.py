"""Redesign social media section: separate profiles from document links, clean layout."""
path = '/opt/wdws/dashboard/osint_static/osint.js'
content = open(path).read()

old = """    // ── Social Media ──
    if (data.social_media.length) {
      h += '<div class="section-title">🌐 Social Media</div>';
      h += '<div class="osint-contact-grid">';
      for (const s of data.social_media) {
        h += `<div class="osint-contact-item">
          ${osintBadge(s.platform, 'cyan')} ${esc(s.username || s.profile_url || '')}
          ${s.profile_url ? `<a href="${esc(s.profile_url)}" target="_blank" style="font-size:.8rem">↗</a>` : ''}
        </div>`;
      }
      h += '</div>';
    }"""

new = """    // ── Social Media ──
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
    }"""

if old in content:
    content = content.replace(old, new)
    print("Fixed JS: redesigned social media section")
else:
    print("ERROR: could not find social media block")

open(path, 'w').write(content)

# ── Add CSS for social chips ──
css_path = '/opt/wdws/dashboard/app.py'
app = open(css_path).read()

social_css = """
.osint-social-profiles { display:flex; flex-wrap:wrap; gap:8px; margin-bottom:1rem; }
.osint-social-chip { display:inline-flex; align-items:center; gap:6px; padding:6px 12px; border-radius:20px;
  background:rgba(255,255,255,0.06); border:1px solid rgba(255,255,255,0.1); text-decoration:none;
  color:var(--fg); font-size:.85rem; transition:all .15s; }
.osint-social-chip:hover { background:rgba(0,200,255,0.12); border-color:var(--accent); }
.osint-social-icon { font-size:1.1rem; }
.osint-social-platform { text-transform:capitalize; color:var(--muted); font-size:.75rem; }
.osint-social-name { font-weight:500; }
"""

# Insert before the closing </style> in the OSINT CSS section
# Find the last osint CSS rule and add after it
if '.osint-social-profiles' not in app:
    # Add before the closing of the OSINT CSS block - find the last osint class
    marker = '.osint-note-author'
    if marker in app:
        idx = app.index(marker)
        # Find the end of that rule (next })
        brace = app.index('}', idx)
        app = app[:brace+1] + social_css + app[brace+1:]
        print("Added social chip CSS")
    else:
        # Try another marker
        marker2 = '.osint-timeline-dot'
        if marker2 in app:
            idx = app.index(marker2)
            brace = app.index('}', idx)
            app = app[:brace+1] + social_css + app[brace+1:]
            print("Added social chip CSS (alt marker)")
        else:
            print("WARNING: could not find CSS insertion point")
else:
    print("Social CSS already present")

open(css_path, 'w').write(app)
print("Done.")
