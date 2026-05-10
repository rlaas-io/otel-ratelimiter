/* ══════════════════════════════════════════════════════════════
   OTEL Rate Limiter Admin — app.js
   ══════════════════════════════════════════════════════════════ */

'use strict';

// ── State ────────────────────────────────────────────────────
var state = {
  health:   null,
  stats:    null,
  config:   null,
  policies: null,
  online:   false,
  tab:      'overview',
};
var refreshCountdown = 10;
var refreshInterval  = null;

// ── DOM helpers ──────────────────────────────────────────────
function $  (id)     { return document.getElementById(id); }
function el (tag, cls, html) {
  var e = document.createElement(tag);
  if (cls)  e.className   = cls;
  if (html) e.innerHTML   = html;
  return e;
}
function txt(tag, cls, content) {
  var e = document.createElement(tag);
  if (cls)     e.className   = cls;
  if (content) e.textContent = content;
  return e;
}

// ── Settings ─────────────────────────────────────────────────
function getBase() {
  return ($('baseURL').value || window.location.origin).replace(/\/$/, '');
}
function getHeaders() {
  var t = ($('authToken').value || '').trim();
  return t ? { 'Authorization': 'Bearer ' + t } : {};
}
function saveSettings() {
  sessionStorage.setItem('otel-rl-base',  $('baseURL').value);
  sessionStorage.setItem('otel-rl-token', $('authToken').value);
}
function loadSettings() {
  var base = sessionStorage.getItem('otel-rl-base') || window.location.origin;
  $('baseURL').value  = base;
  var tok = sessionStorage.getItem('otel-rl-token') || '';
  $('authToken').value = tok;
}

// ── Number formatting ────────────────────────────────────────
function fmt(n) {
  if (n == null || n === '') return '—';
  n = Number(n);
  if (n >= 1e9) return (n / 1e9).toFixed(1) + 'B';
  if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
  if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
  return String(n);
}
function fmtFull(n) {
  if (n == null) return '—';
  return Number(n).toLocaleString();
}
function fmtUptime(s) {
  s = Math.round(s || 0);
  if (s < 60)   return s + 's';
  if (s < 3600) return Math.floor(s / 60) + 'm ' + (s % 60) + 's';
  var h = Math.floor(s / 3600);
  var m = Math.floor((s % 3600) / 60);
  return h + 'h ' + m + 'm';
}
function dropPct(received, dropped) {
  if (!received || received === 0) return 0;
  return Math.round((dropped / received) * 1000) / 10;
}
function dropClass(pct) {
  return pct >= 20 ? 'danger' : pct >= 5 ? 'warn' : 'ok';
}
function dropColor(pct) {
  return pct >= 20 ? '#ef4444' : pct >= 5 ? '#f59e0b' : '#22c55e';
}

// ── JSON syntax highlighter ──────────────────────────────────
function jsonHL(str) {
  str = str
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
  // Keys: "text":
  str = str.replace(/"((?:[^"\\]|\\.)*)"\s*:/g, '<span class="jk">"$1"</span>:');
  // String values after colon
  str = str.replace(/:\s*"((?:[^"\\]|\\.)*)"/g, function(m, v) {
    return ': <span class="js">"' + v + '"</span>';
  });
  // Booleans
  str = str.replace(/\b(true|false)\b/g, '<span class="jb">$1</span>');
  // Null
  str = str.replace(/\bnull\b/g, '<span class="jnull">null</span>');
  // Numbers (only after : or in arrays)
  str = str.replace(/([:,\[])\s*(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)\b/g, function(m, p, n) {
    return p + ' <span class="jn">' + n + '</span>';
  });
  return str;
}

// ── API calls ────────────────────────────────────────────────
async function fetchAll() {
  var base    = getBase();
  var headers = getHeaders();
  var results = await Promise.allSettled([
    fetch(base + '/health',          { headers: headers }),
    fetch(base + '/stats',           { headers: headers }),
    fetch(base + '/config',          { headers: headers }),
    fetch(base + '/config/policies', { headers: headers }),
  ]);

  var ok = false;
  try {
    if (results[0].status === 'fulfilled' && results[0].value.ok) {
      state.health = await results[0].value.json();
      ok = true;
    } else { state.health = null; }
  } catch(e) { state.health = null; }

  try {
    if (results[1].status === 'fulfilled' && results[1].value.ok)
      state.stats = await results[1].value.json();
    else state.stats = null;
  } catch(e) { state.stats = null; }

  try {
    if (results[2].status === 'fulfilled' && results[2].value.ok)
      state.config = await results[2].value.json();
    else state.config = null;
  } catch(e) { state.config = null; }

  try {
    if (results[3].status === 'fulfilled' && results[3].value.ok)
      state.policies = await results[3].value.json();
    else state.policies = null;
  } catch(e) { state.policies = null; }

  state.online = ok;
  setStatus(ok);
  renderActive();
}

// ── Status indicator ─────────────────────────────────────────
function setStatus(online) {
  var dot  = $('statusDot');
  var text = $('statusText');
  if (online) {
    dot.className  = 'status-dot online';
    text.textContent = 'Online';
  } else {
    dot.className  = 'status-dot offline';
    text.textContent = 'Offline';
  }
}

// ── Tab navigation ───────────────────────────────────────────
function navigate(tab) {
  state.tab = tab;
  document.querySelectorAll('.nav-item').forEach(function(b) {
    b.classList.toggle('active', b.dataset.tab === tab);
  });
  document.querySelectorAll('.tab').forEach(function(s) {
    s.classList.toggle('active', s.id === 'tab-' + tab);
  });
  renderActive();
}

function renderActive() {
  switch (state.tab) {
    case 'overview': renderOverview(); break;
    case 'stats':    renderStats();    break;
    case 'config':   renderConfig();   break;
    case 'policies': renderPolicies(); break;
    case 'actions':  renderActions();  break;
  }
}

// ═══════════════════════════════════════════════════════════════
// RENDER: OVERVIEW
// ═══════════════════════════════════════════════════════════════
function renderOverview() {
  var h = state.health;
  var s = state.stats;

  // Aggregate totals from stats
  var totalReceived = 0, totalDropped = 0, signals = 0;
  if (s) {
    Object.keys(s).forEach(function(k) {
      var sig = s[k];
      totalReceived += (sig.received || 0);
      totalDropped  += (sig.dropped  || 0);
      signals++;
    });
  }
  var totalDrop = dropPct(totalReceived, totalDropped);

  // ── KPI cards ──
  var kpiGrid = $('kpiGrid');
  kpiGrid.innerHTML = '';

  var kpis = [
    {
      cls: state.online ? 'kpi-green' : 'kpi-red',
      iconCls: state.online ? 'icon-green' : '',
      icon: '<path d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" fill-rule="evenodd" clip-rule="evenodd"/>',
      value: state.online ? 'Online' : 'Offline',
      label: 'Status',
      sub:   h ? 'Uptime: ' + fmtUptime(h.uptime_seconds) : '',
    },
    {
      cls: 'kpi-sky',
      iconCls: 'icon-sky',
      icon: '<path fill-rule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm1-12a1 1 0 10-2 0v4a1 1 0 00.293.707l2.828 2.829a1 1 0 101.415-1.415L11 9.586V6z" clip-rule="evenodd"/>',
      value: h ? fmtUptime(h.uptime_seconds) : '—',
      label: 'Uptime',
      sub:   '',
    },
    {
      cls: 'kpi-purple',
      iconCls: 'icon-purple',
      icon: '<path d="M13 6a3 3 0 11-6 0 3 3 0 016 0zM18 8a2 2 0 11-4 0 2 2 0 014 0zM14 15a4 4 0 00-8 0v3h8v-3zM6 8a2 2 0 11-4 0 2 2 0 014 0zM16 18v-3a5.972 5.972 0 00-.75-2.906A3.005 3.005 0 0119 15v3h-3zM4.75 12.094A5.973 5.973 0 004 15v3H1v-3a3 3 0 013.75-2.906z"/>',
      value: s ? String(signals) : '—',
      label: 'Active Signals',
      sub:   s ? Object.keys(s).join(', ') : '',
    },
    {
      cls: 'kpi-sky',
      iconCls: 'icon-sky',
      icon: '<path fill-rule="evenodd" d="M3 3a1 1 0 000 2v8a2 2 0 002 2h2.586l-1.293 1.293a1 1 0 101.414 1.414L10 15.414l2.293 2.293a1 1 0 001.414-1.414L12.414 15H15a2 2 0 002-2V5a1 1 0 100-2H3zm11 4a1 1 0 10-2 0v4a1 1 0 102 0V7zm-3 1a1 1 0 10-2 0v3a1 1 0 102 0V8zM8 9a1 1 0 00-2 0v2a1 1 0 102 0V9z" clip-rule="evenodd"/>',
      value: fmt(totalReceived),
      label: 'Total Received',
      sub:   fmtFull(totalReceived) + ' records',
    },
    {
      cls: totalDrop >= 20 ? 'kpi-red' : totalDrop >= 5 ? 'kpi-amber' : 'kpi-green',
      iconCls: totalDrop >= 20 ? '' : totalDrop >= 5 ? 'icon-amber' : 'icon-green',
      icon: '<path fill-rule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clip-rule="evenodd"/>',
      value: totalDrop + '%',
      label: 'Drop Rate',
      sub:   fmt(totalDropped) + ' records dropped',
    },
  ];

  kpis.forEach(function(kpi) {
    var card = el('div', 'kpi-card ' + kpi.cls);
    var icon = el('div', 'kpi-icon ' + kpi.iconCls);
    icon.innerHTML = '<svg viewBox="0 0 20 20" fill="currentColor" width="16" height="16">' + kpi.icon + '</svg>';
    var val  = txt('div', 'kpi-value', kpi.value);
    var lbl  = txt('div', 'kpi-label', kpi.label);
    card.appendChild(icon);
    card.appendChild(val);
    card.appendChild(lbl);
    if (kpi.sub) {
      var sub = txt('div', 'kpi-sub', kpi.sub);
      card.appendChild(sub);
    }
    kpiGrid.appendChild(card);
  });

  // ── Signal breakdown ──
  var grid = $('signalGrid');
  grid.innerHTML = '';
  if (!s || Object.keys(s).length === 0) {
    grid.innerHTML = '<div class="signal-card"><div style="padding:.5rem;color:var(--t3);font-size:.85rem;">No signal data yet — the collector may not have processed any records.</div></div>';
    return;
  }
  Object.keys(s).forEach(function(sig) {
    var d = s[sig];
    var pct = dropPct(d.received, d.dropped);
    var cls = dropClass(pct);
    var sigCls = sig === 'logs' || sig === 'log' ? 'sig-logs' : sig === 'traces' || sig === 'span' ? 'sig-span' : sig === 'metrics' || sig === 'metric' ? 'sig-metric' : 'sig-unknown';

    var card = el('div', 'signal-card');

    var badge = el('span', 'signal-badge ' + sigCls, sig.toUpperCase());
    card.appendChild(badge);

    var stats = el('div', 'signal-stats');
    [
      { label: 'Received', value: fmt(d.received) },
      { label: 'Allowed',  value: fmt(d.allowed)  },
      { label: 'Denied',   value: fmt(d.denied),   cls: 'sig-stat-value denied' },
      { label: 'Shadow',   value: fmt(d.shadow),   cls: 'sig-stat-value shadow' },
    ].forEach(function(col) {
      var cell = el('div');
      cell.appendChild(txt('div', 'sig-stat-label', col.label));
      cell.appendChild(txt('div', col.cls || 'sig-stat-value', col.value));
      stats.appendChild(cell);
    });
    card.appendChild(stats);

    var pw = el('div', 'progress-wrap');
    var track = el('div', 'progress-track');
    var fill  = el('div', 'progress-fill ' + cls);
    fill.style.width = Math.min(pct, 100) + '%';
    track.appendChild(fill);
    var lbl = txt('span', 'progress-label ' + cls, pct + '% drop');
    pw.appendChild(track);
    pw.appendChild(lbl);
    card.appendChild(pw);

    grid.appendChild(card);
  });
}

// ═══════════════════════════════════════════════════════════════
// RENDER: STATS
// ═══════════════════════════════════════════════════════════════
function renderStats() {
  var statsCard = $('statsCard');
  var s = state.stats;

  if (!s) {
    statsCard.innerHTML = '<div class="empty-state">No stats available — is the admin API reachable?</div>';
    return;
  }

  var keys = Object.keys(s);
  if (keys.length === 0) {
    statsCard.innerHTML = '<div class="empty-state">No signals registered yet.</div>';
    return;
  }

  var table = el('table', 'stats-table');
  var thead = el('thead');
  thead.innerHTML = '<tr>' +
    '<th>Signal</th>' +
    '<th>Received</th>' +
    '<th>Allowed</th>' +
    '<th>Denied</th>' +
    '<th>Shadow</th>' +
    '<th>Errors</th>' +
    '<th>Reloads</th>' +
    '<th>Drop Rate</th>' +
    '</tr>';
  table.appendChild(thead);

  var tbody = el('tbody');
  keys.forEach(function(sig) {
    var d = s[sig];
    var pct = dropPct(d.received, d.dropped);
    var cls = dropClass(pct);
    var color = dropColor(pct);
    var sigCls = sig === 'logs' || sig === 'log' ? 'sig-logs' : sig === 'traces' || sig === 'span' ? 'sig-span' : sig === 'metrics' || sig === 'metric' ? 'sig-metric' : 'sig-unknown';

    var tr = el('tr');
    tr.innerHTML =
      '<td><span class="signal-badge ' + sigCls + '">' + sig + '</span></td>' +
      '<td>' + fmtFull(d.received) + '</td>' +
      '<td>' + fmtFull(d.allowed)  + '</td>' +
      '<td>' + fmtFull(d.denied)   + '</td>' +
      '<td>' + fmtFull(d.shadow)   + '</td>' +
      '<td>' + fmtFull(d.errors)   + '</td>' +
      '<td>' + fmtFull(d.reloads)  + '</td>' +
      '<td>' +
        '<div class="drop-cell">' +
          '<div class="drop-bar-track"><div class="drop-bar-fill" style="width:' + Math.min(pct,100) + '%;background:' + color + '"></div></div>' +
          '<span class="drop-pct ' + cls + '">' + pct + '%</span>' +
        '</div>' +
      '</td>';
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);
  statsCard.innerHTML = '';
  statsCard.appendChild(table);
}

// ═══════════════════════════════════════════════════════════════
// RENDER: CONFIGURATION
// ═══════════════════════════════════════════════════════════════
function renderConfig() {
  var container = $('configContent');
  var cfg = state.config;

  if (!cfg) {
    container.innerHTML = '<div class="card"><div class="empty-state">Configuration not available.</div></div>';
    return;
  }

  var groups = [
    {
      title: 'Policies',
      rows: [
        ['policy_file',     cfg.policy_file     || '',         false],
        ['policies_inline', cfg.policies_inline ? 'set (inline)' : 'not set', false],
        ['watch_policies',  cfg.watch_policies,               true],
        ['watch_interval',  cfg.watch_interval  || '—',       false],
        ['cache_ttl',       cfg.cache_ttl       || '—',       false],
      ],
    },
    {
      title: 'Context Defaults',
      rows: [
        ['key_prefix',   cfg.key_prefix   || '—', false],
        ['org_id',       cfg.org_id       || '—', false],
        ['tenant_id',    cfg.tenant_id    || '—', false],
        ['application',  cfg.application  || '—', false],
        ['environment',  cfg.environment  || '—', false],
      ],
    },
    {
      title: 'Expressions',
      rows: [
        ['service_expr',     cfg.service_expr     || '—', false],
        ['org_id_expr',      cfg.org_id_expr      || '—', false],
        ['tenant_id_expr',   cfg.tenant_id_expr   || '—', false],
        ['application_expr', cfg.application_expr || '—', false],
        ['environment_expr', cfg.environment_expr || '—', false],
      ],
    },
    {
      title: 'Behavior',
      rows: [
        ['fail_open',      cfg.fail_open,                true],
        ['max_batch_size', cfg.max_batch_size === 0 ? 'unlimited (0)' : String(cfg.max_batch_size), false],
      ],
    },
    {
      title: 'Admin API',
      rows: [
        ['admin_addr',               cfg.admin_addr        || '—', false],
        ['admin_auth_enabled',       cfg.admin_auth_enabled,       true],
        ['admin_token_header',       cfg.admin_token_header || '—',false],
        ['admin_tls_enabled',        cfg.admin_tls_enabled,        true],
        ['admin_tls_client_ca',      cfg.admin_tls_client_ca,      true],
        ['admin_cors_allowed_origins', cfg.admin_cors_allowed_origins && cfg.admin_cors_allowed_origins.length
            ? cfg.admin_cors_allowed_origins.join(', ') : '—', false],
      ],
    },
  ];

  container.innerHTML = '';
  groups.forEach(function(group) {
    var section = el('div', 'card config-section');
    var title   = txt('div', 'config-section-title', group.title);
    section.appendChild(title);

    group.rows.forEach(function(row) {
      var key   = row[0];
      var val   = row[1];
      var isBool= row[2];

      var rowDiv = el('div', 'config-row');
      var keyEl  = txt('span', 'config-key', key);
      rowDiv.appendChild(keyEl);

      if (isBool) {
        var pill = el('span', val ? 'pill pill-true' : 'pill pill-false', val ? 'true' : 'false');
        rowDiv.appendChild(pill);
      } else {
        var valEl = el('span', val === '—' ? 'config-val empty' : 'config-val');
        valEl.textContent = String(val);
        rowDiv.appendChild(valEl);
      }

      section.appendChild(rowDiv);
    });

    container.appendChild(section);
  });
}

// ═══════════════════════════════════════════════════════════════
// RENDER: POLICIES
// ═══════════════════════════════════════════════════════════════
function renderPolicies() {
  var p = state.policies;

  // Meta cards
  var metaGrid = $('policyMetaGrid');
  if (p) {
    var metas = [
      { label: 'Source',         value: p.source || '—',         mono: false },
      { label: 'Policy File',    value: p.policy_file || '(inline)', mono: true },
      { label: 'Policy Count',   value: String(p.policy_count != null ? p.policy_count : '—'), mono: false },
      { label: 'SHA-256',        value: p.content_sha256 ? p.content_sha256.slice(0,16) + '…' : '—', mono: true },
      { label: 'Last Modified',  value: p.last_modified || '—',   mono: false },
    ];
    metaGrid.innerHTML = '';
    metas.forEach(function(m) {
      var card  = el('div', 'meta-card');
      var label = txt('div', 'meta-label', m.label);
      var value = txt('div', m.mono ? 'meta-value mono' : 'meta-value', m.value);
      card.appendChild(label);
      card.appendChild(value);
      metaGrid.appendChild(card);
    });
  } else {
    metaGrid.innerHTML = '';
  }

  // JSON view
  var code = $('policyCode');
  if (p && p.policies != null) {
    var pretty = JSON.stringify(p.policies, null, 2);
    code.innerHTML = jsonHL(pretty);
  } else {
    code.innerHTML = 'No policy data available.';
  }

  // Copy button
  var copyBtn = $('copyPoliciesBtn');
  copyBtn.onclick = function() {
    if (!p || p.policies == null) return;
    navigator.clipboard.writeText(JSON.stringify(p.policies, null, 2)).then(function() {
      copyBtn.textContent = '✓ Copied';
      copyBtn.classList.add('copied');
      setTimeout(function() {
        copyBtn.textContent = 'Copy';
        copyBtn.classList.remove('copied');
      }, 2000);
    });
  };
}

// ═══════════════════════════════════════════════════════════════
// RENDER: ACTIONS
// ═══════════════════════════════════════════════════════════════
function renderActions() {
  // Reload button (set up once, re-set onclick each render)
  var reloadBtn = $('reloadBtn');
  reloadBtn.onclick = doReload;

  // Explorer list
  var list = $('explorerList');
  if (list.children.length > 0) return; // already rendered

  var endpoints = [
    { method: 'GET',  path: '/health',          desc: 'Liveness probe — returns uptime_seconds.' },
    { method: 'GET',  path: '/stats',           desc: 'Cumulative per-signal counters (JSON).' },
    { method: 'GET',  path: '/config',          desc: 'Sanitized active processor configuration.' },
    { method: 'GET',  path: '/config/policies', desc: 'Active policy payload with checksum and metadata.' },
    { method: 'POST', path: '/reload',          desc: 'Force immediate policy reload on all engines.' },
    { method: 'GET',  path: '/metrics',         desc: 'Prometheus/OpenMetrics text format scrape.' },
  ];

  endpoints.forEach(function(ep, idx) {
    var card = el('div', 'explorer-card');

    // Header row
    var header = el('div', 'explorer-header');
    var badge  = el('span', 'method-badge method-' + ep.method.toLowerCase(), ep.method);
    var path   = txt('span', 'explorer-path', ep.path);
    var desc   = txt('span', 'explorer-desc', ep.desc);
    header.appendChild(badge);
    header.appendChild(path);
    header.appendChild(desc);
    card.appendChild(header);

    // Action row
    var actions  = el('div', 'explorer-actions');
    var btnCall  = el('button', 'btn-call', 'Call');
    var btnCurl  = el('button', 'btn-curl', 'Copy curl');
    var resultEl = el('span', 'explorer-result', '');
    actions.appendChild(btnCall);
    actions.appendChild(btnCurl);
    actions.appendChild(resultEl);
    card.appendChild(actions);

    // Response area
    var respEl = el('pre', 'explorer-response');
    card.appendChild(respEl);

    // Handlers
    btnCall.onclick = function() {
      callEndpoint(ep.method, ep.path, resultEl, respEl);
    };
    btnCurl.onclick = function() {
      copyCurl(ep.method, ep.path, btnCurl);
    };

    list.appendChild(card);
  });
}

async function doReload() {
  var btn    = $('reloadBtn');
  var result = $('reloadResult');
  btn.disabled = true;
  btn.innerHTML = '<span style="opacity:.7">↺&nbsp; Reloading…</span>';
  result.textContent = '';
  result.className = 'reload-result';

  try {
    var res  = await fetch(getBase() + '/reload', { method: 'POST', headers: getHeaders() });
    var data = await res.json();
    result.textContent = '✓ Reloaded ' + data.engines + ' engine(s)';
    result.className = 'reload-result ok';
    fetchAll();
  } catch (err) {
    result.textContent = '✗ ' + err.message;
    result.className = 'reload-result err';
  } finally {
    btn.disabled = false;
    btn.innerHTML = '↺ &nbsp;Reload Now';
  }
}

async function callEndpoint(method, path, resultEl, respEl) {
  resultEl.textContent = 'Calling…';
  respEl.textContent   = '';
  respEl.classList.remove('visible', 'error');

  try {
    var res  = await fetch(getBase() + path, { method: method, headers: getHeaders() });
    var text = await res.text();
    resultEl.textContent = 'HTTP ' + res.status;

    // Try to pretty-print JSON
    var formatted = text;
    try { formatted = JSON.stringify(JSON.parse(text), null, 2); } catch(e) {}

    respEl.textContent = formatted;
    respEl.classList.add('visible');
    if (!res.ok) respEl.classList.add('error');
  } catch (err) {
    resultEl.textContent = 'Error';
    respEl.textContent   = err.message;
    respEl.classList.add('visible', 'error');
  }
}

function copyCurl(method, path, btn) {
  var base    = getBase();
  var token   = ($('authToken').value || '').trim();
  var authStr = token ? ' \\\n  -H "Authorization: Bearer ' + token + '"' : '';
  var mFlag   = method !== 'GET' ? ' -X ' + method : '';
  var cmd     = 'curl -s' + mFlag + ' "' + base + path + '"' + authStr;

  navigator.clipboard.writeText(cmd).then(function() {
    btn.textContent = '✓ Copied';
    btn.classList.add('copied');
    setTimeout(function() {
      btn.textContent = 'Copy curl';
      btn.classList.remove('copied');
    }, 2000);
  });
}

// ══════════════════════════════════════════════════════════════
// AUTO-REFRESH TIMER
// ══════════════════════════════════════════════════════════════
function startRefreshTimer() {
  stopRefreshTimer();
  refreshCountdown = 10;
  updateCountdown();
  refreshInterval = setInterval(function() {
    refreshCountdown--;
    updateCountdown();
    if (refreshCountdown <= 0) {
      fetchAll();
      refreshCountdown = 10;
    }
  }, 1000);
}
function stopRefreshTimer() {
  if (refreshInterval) { clearInterval(refreshInterval); refreshInterval = null; }
}
function updateCountdown() {
  var hint = $('refreshHint');
  if (hint) hint.textContent = 'Refresh in ' + refreshCountdown + 's';
}

// ══════════════════════════════════════════════════════════════
// INIT
// ══════════════════════════════════════════════════════════════
function init() {
  loadSettings();

  // Save settings on change
  $('baseURL').addEventListener('change',  saveSettings);
  $('authToken').addEventListener('change', saveSettings);

  // Manual refresh
  $('refreshBtn').addEventListener('click', function() {
    fetchAll();
    startRefreshTimer();
  });

  // Tab navigation
  document.getElementById('sideNav').addEventListener('click', function(e) {
    var btn = e.target.closest('[data-tab]');
    if (btn) navigate(btn.dataset.tab);
  });

  // Refetch when settings change (re-detect base URL)
  $('baseURL').addEventListener('change', function() { fetchAll(); startRefreshTimer(); });

  // Initial fetch
  fetchAll();
  startRefreshTimer();
}

document.addEventListener('DOMContentLoaded', init);
