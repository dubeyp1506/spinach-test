'use strict';

const CHANNELS = ['email', 'sms', 'whatsapp', 'push', 'web'];
const TYPES = ['sent', 'delivered', 'opened', 'clicked', 'converted', 'bounced', 'unsubscribed', 'complained'];

/* ---------- helpers ---------- */

const $ = (s, r = document) => r.querySelector(s);
const esc = s => String(s ?? '').replace(/[&<>"']/g, c =>
  ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const isObj = v => v && typeof v === 'object' && !Array.isArray(v);
const base = () => $('#apiBase').value.replace(/\/+$/, '') || '/api/v1';

// BFS field lookup: top-level scalar match first, then one level of nested
// objects. Tolerates either flat or nested response shapes.
function dpick(o, keys) {
  if (!isObj(o)) return undefined;
  let objMatch;
  for (const k of keys) {
    if (o[k] === undefined || o[k] === null) continue;
    if (isObj(o[k])) { if (objMatch === undefined) objMatch = o[k]; continue; }
    return o[k];
  }
  for (const v of Object.values(o)) {
    if (!isObj(v)) continue;
    for (const k of keys) {
      if (v[k] === undefined || v[k] === null) continue;
      if (isObj(v[k])) { if (objMatch === undefined) objMatch = v[k]; continue; }
      return v[k];
    }
  }
  return objMatch;
}

async function api(path, opts = {}) {
  const res = await fetch(base() + path, {
    ...opts,
    headers: { 'Content-Type': 'application/json', ...(opts.headers || {}) },
  });
  let body = null;
  try { body = await res.json(); } catch (_) { /* non-JSON error body */ }
  if (!res.ok) {
    throw new Error((body && (body.message || body.error)) || `HTTP ${res.status} ${res.statusText}`);
  }
  return body;
}

// Run fn with the view's loading indicator + error envelope display.
async function busy(view, fn) {
  const sec = $('#view-' + view);
  const errEl = sec.querySelector('.error');
  errEl.textContent = '';
  sec.classList.add('busy');
  try { await fn(); }
  catch (e) { errEl.textContent = e.message || String(e); }
  finally { sec.classList.remove('busy'); }
}

const fmtNum = v => (typeof v === 'number' ? v.toLocaleString() : esc(v ?? '—'));
const fmtScore = v => (typeof v === 'number' ? v.toFixed(3) : esc(v ?? '—'));
const fmtRate = v => {
  if (typeof v !== 'number') return esc(v ?? '—');
  return (v <= 1 ? v * 100 : v).toFixed(1) + '%';
};
const fmtTime = s => {
  if (!s) return '—';
  const d = new Date(s);
  return isNaN(d) ? esc(s) : d.toLocaleString();
};
const badge = (txt, cls = '') => `<span class="badge ${cls}">${esc(txt)}</span>`;

function statusBadge(v) {
  const s = String(isObj(v) ? (v.status ?? v.state ?? '') : v).toLowerCase();
  const ok = v === true || ['up', 'ok', 'healthy', 'connected', 'ready'].includes(s);
  return badge(s || String(v), ok ? 'ok' : 'bad');
}

function table(headers, rows) {
  return `<table><thead><tr>${headers.map(h => `<th>${esc(h)}</th>`).join('')}</tr></thead>` +
    `<tbody>${rows.map(r => `<tr>${r.map(c => `<td>${c}</td>`).join('')}</tr>`).join('')}</tbody></table>`;
}

const jsonBlock = o =>
  `<details><summary>raw JSON</summary><pre>${esc(JSON.stringify(o, null, 2))}</pre></details>`;

const metricCell = (k, v) =>
  `<span class="num">${typeof v === 'number' ? (k.includes('rate') ? fmtRate(v) : fmtNum(v)) : esc(v ?? '—')}</span>`;

/* ---------- nav ---------- */

document.querySelectorAll('.nav').forEach(b => b.addEventListener('click', () => {
  document.querySelectorAll('.nav').forEach(x => x.classList.toggle('active', x === b));
  document.querySelectorAll('.view').forEach(v =>
    v.classList.toggle('active', v.id === 'view-' + b.dataset.view));
  if (b.dataset.view === 'campaigns' && !campLoaded) loadCampaigns();
  if (b.dataset.view === 'system' && !sysLoaded) loadSystem();
  if (b.dataset.view === 'logs' && !logsLoaded) loadLogs();
}));

function showView(view) {
  document.querySelector(`.nav[data-view="${view}"]`).click();
}

/* ---------- 1. Ingest ---------- */

function randomEvent() {
  return {
    event_id: 'evt_' + Date.now().toString(36) + Math.random().toString(36).slice(2, 8),
    customer_id: 'cust_' + String(1 + Math.floor(Math.random() * 200)).padStart(5, '0'),
    campaign_id: 'camp_' + String(1 + Math.floor(Math.random() * 20)).padStart(3, '0'),
    channel: CHANNELS[Math.floor(Math.random() * CHANNELS.length)],
    type: TYPES[Math.floor(Math.random() * TYPES.length)],
    occurred_at: new Date(Date.now() - Math.floor(Math.random() * 7 * 86400000)).toISOString(),
    payload: { source: 'demo-ui' },
  };
}

$('#ingBody').value = JSON.stringify({ events: [randomEvent(), randomEvent()] }, null, 2);

$('#ingRandom').onclick = () => {
  $('#ingBody').value = JSON.stringify({ events: [randomEvent()] }, null, 2);
};

let lastIngestRequestId = null;

$('#ingSubmit').onclick = () => busy('ingest', async () => {
  let payload;
  try { payload = JSON.parse($('#ingBody').value); }
  catch (e) { throw new Error('Invalid JSON: ' + e.message); }
  // Our own X-Request-ID, so the batch can be traced end to end in Logs.
  const reqId = 'ui-' + Date.now().toString(36) + '-' + Math.random().toString(36).slice(2, 6);
  const r = await api('/events', {
    method: 'POST', body: JSON.stringify(payload), headers: { 'X-Request-ID': reqId },
  });
  lastIngestRequestId = reqId;
  $('#ingTrace').classList.remove('hidden');
  const rej = r.rejected || [];
  $('#ingResult').innerHTML = `
    <div class="cards">
      <div class="card"><div class="k">accepted</div><div class="v num ok">${fmtNum(r.accepted ?? 0)}</div></div>
      <div class="card"><div class="k">duplicates</div><div class="v num">${fmtNum(r.duplicates ?? 0)}</div></div>
      <div class="card"><div class="k">rejected</div><div class="v num ${rej.length ? 'bad' : ''}">${rej.length}</div></div>
    </div>
    ${rej.length ? table(['index', 'reason'], rej.map(x =>
      [`<span class="num">${esc(x.index)}</span>`, esc(x.reason)])) : ''}
    ${jsonBlock(r)}`;
});

$('#ingTrace').onclick = () => {
  setLogFilters({ request_id: lastIngestRequestId });
  showView('logs');
  loadLogs();
};

/* ---------- 2. Customers ---------- */

let tlCursor = null;

$('#custLoad').onclick = () => busy('customers', async () => {
  const id = encodeURIComponent($('#custId').value.trim());
  tlCursor = null;
  const p = await api(`/customers/${id}`);
  renderProfile(p);
  try {
    renderTimeline(await api(`/customers/${id}/timeline?limit=25`), true);
  } catch (e) {
    $('#custTimeline').innerHTML = '';
    $('#tlMore').classList.add('hidden');
    throw e;
  }
});

$('#tlMore').onclick = () => busy('customers', async () => {
  const id = encodeURIComponent($('#custId').value.trim());
  renderTimeline(await api(
    `/customers/${id}/timeline?limit=25&cursor=${encodeURIComponent(tlCursor)}`), false);
});

function renderProfile(p) {
  const score = dpick(p, ['score']);
  const raw = dpick(p, ['raw_score', 'engagement_score']);
  const trend = dpick(p, ['trend', 'activity_trend']);
  const chan = dpick(p, ['preferred_channel']);
  const cc = dpick(p, ['channel_counts']) || {};
  const trendCls = trend === 'rising' ? 'ok' : trend === 'declining' ? 'bad' : '';
  const countKeys = ['total_events', 'positive_events', 'negative_events', 'conversions'];
  const types = [...new Set(Object.values(cc).flatMap(m => isObj(m) ? Object.keys(m) : []))];
  $('#custProfile').innerHTML = `
    <div class="cards">
      <div class="card"><div class="k">score</div><div class="v num">${fmtScore(score)}</div></div>
      <div class="card"><div class="k">raw_score</div><div class="v num">${fmtScore(raw)}</div></div>
      <div class="card"><div class="k">trend</div><div class="v">${trend ? badge(trend, trendCls) : '—'}</div></div>
      <div class="card"><div class="k">preferred_channel</div><div class="v">${chan ? badge(chan) : '—'}</div></div>
      ${countKeys.map(k => `<div class="card"><div class="k">${k}</div><div class="v num">${fmtNum(dpick(p, [k]))}</div></div>`).join('')}
    </div>
    ${types.length ? `<h4>channel_counts</h4>` + table(['channel', ...types],
      Object.entries(cc).map(([ch, m]) =>
        [badge(ch), ...types.map(t => `<span class="num">${isObj(m) ? (m[t] ?? 0) : 0}</span>`)])) : ''}
    ${jsonBlock(p)}`;
}

function renderTimeline(t, reset) {
  const rows = (t.data || []).map(e => `<tr>
    <td>${fmtTime(e.occurred_at)}</td>
    <td>${badge(e.channel)}</td>
    <td>${esc(e.type)}</td>
    <td class="mono">${esc(e.event_id ?? '')}</td>
    <td class="mono">${esc(e.campaign_id ?? '')}</td>
  </tr>`);
  if (reset || !$('#tlTable')) {
    $('#custTimeline').innerHTML = `<table id="tlTable"><thead><tr>
      <th>occurred_at</th><th>channel</th><th>type</th><th>event_id</th><th>campaign_id</th>
    </tr></thead><tbody></tbody></table>`;
  }
  $('#tlTable tbody').insertAdjacentHTML('beforeend', rows.join(''));
  tlCursor = t.next_cursor || null;
  $('#tlMore').classList.toggle('hidden', !t.has_more);
}

/* ---------- 3. Campaigns ---------- */

let campCursor = null, campLoaded = false;

function loadCampaigns() {
  campLoaded = true;
  return busy('campaigns', async () => {
    campCursor = null;
    renderCampaigns(await api('/campaigns?limit=50'), true);
  });
}

$('#campLoad').onclick = loadCampaigns;

$('#campMore').onclick = () => busy('campaigns', async () => {
  renderCampaigns(await api(
    `/campaigns?limit=50&cursor=${encodeURIComponent(campCursor)}`), false);
});

function renderCampaigns(r, reset) {
  const rows = (r.data || []).map(c => {
    const id = c.external_id ?? c.id ?? '';
    return `<tr data-id="${esc(id)}" class="clickable">
      <td class="mono">${esc(id)}</td>
      <td>${esc(c.name)}</td>
      <td>${badge(c.channel)}</td>
      <td>${esc(c.objective)}</td>
      <td>${badge(c.status)}</td>
      <td class="num">${fmtNum(dpick(c, ['sends']))}</td>
      <td class="num">${fmtRate(dpick(c, ['open_rate']))}</td>
    </tr>`;
  });
  if (reset || !$('#campTable')) {
    $('#campList').innerHTML = `<table id="campTable"><thead><tr>
      <th>id</th><th>name</th><th>channel</th><th>objective</th><th>status</th><th>sends</th><th>open_rate</th>
    </tr></thead><tbody></tbody></table>`;
  }
  $('#campTable tbody').insertAdjacentHTML('beforeend', rows.join(''));
  campCursor = r.next_cursor || null;
  $('#campMore').classList.toggle('hidden', !r.has_more);
}

$('#campList').addEventListener('click', e => {
  const tr = e.target.closest('tr[data-id]');
  if (!tr) return;
  const id = tr.dataset.id;
  busy('campaigns', async () => {
    renderAnalytics(id, await api(`/campaigns/${encodeURIComponent(id)}/analytics`));
  });
});

function renderAnalytics(id, m) {
  const countKeys = ['sends', 'delivered', 'opens', 'clicks', 'conversions', 'bounces', 'unsubscribes'];
  const rateKeys = ['open_rate', 'click_rate', 'conversion_rate'];
  const byCh = m.by_channel || m.byChannel || {};
  const anoms = m.anomalies || [];
  const baseline = m.platform_baseline;
  let chTable = '';
  if (isObj(byCh) && Object.keys(byCh).length) {
    const cols = [...new Set(Object.values(byCh).flatMap(v => isObj(v) ? Object.keys(v) : []))];
    chTable = `<h4>by_channel</h4>` + table(['channel', ...cols],
      Object.entries(byCh).map(([ch, v]) =>
        [badge(ch), ...cols.map(c => metricCell(c, isObj(v) ? v[c] : undefined))]));
  }
  $('#campAnalytics').innerHTML = `
    <h3>Analytics — ${esc(id)}</h3>
    <div class="cards">
      ${countKeys.map(k => `<div class="card"><div class="k">${k}</div><div class="v num">${fmtNum(m[k])}</div></div>`).join('')}
      ${rateKeys.map(k => `<div class="card"><div class="k">${k}</div><div class="v num">${fmtRate(m[k])}</div></div>`).join('')}
    </div>
    ${chTable}
    ${anoms.length ? `<h4>anomalies</h4><ul>${anoms.map(a =>
      `<li>${esc(typeof a === 'string' ? a : JSON.stringify(a))}</li>`).join('')}</ul>` : ''}
    ${isObj(baseline) ? `<h4>platform_baseline</h4>` + table(['metric', 'value'],
      Object.entries(baseline).map(([k, v]) => [esc(k), metricCell(k, v)])) : ''}
    ${jsonBlock(m)}`;
}

/* ---------- 4. Audience ---------- */

$('#audSubmit').onclick = () => busy('audience', async () => {
  const conditions = {};
  if ($('#audMinScore').value !== '') conditions.min_score = parseFloat($('#audMinScore').value);
  if ($('#audDays').value !== '') conditions.last_active_days = parseInt($('#audDays').value, 10);
  const body = {
    objective: $('#audObjective').value,
    channel: $('#audChannel').value,
    size: parseInt($('#audSize').value, 10) || 10,
  };
  if (Object.keys(conditions).length) body.conditions = conditions;
  const r = await api('/audience/recommend', { method: 'POST', body: JSON.stringify(body) });
  const meta = r.meta || {};
  $('#audResult').innerHTML = `
    <p class="meta num">candidates_considered: ${fmtNum(meta.candidates_considered)}
      · filtered_out: ${fmtNum(meta.filtered_out)}
      · took_ms: ${fmtNum(meta.took_ms)}</p>
    ${table(['rank', 'customer_id', 'score', 'reasons'], (r.candidates || []).map(c => [
      `<span class="num">${esc(c.rank)}</span>`,
      `<span class="mono">${esc(c.customer_id)}</span>`,
      `<span class="num">${fmtScore(c.score)}</span>`,
      `<ul class="reasons">${(c.reasons || []).map(x => `<li>${esc(x)}</li>`).join('')}</ul>`,
    ]))}
    ${jsonBlock(r)}`;
});

/* ---------- Channel prediction ---------- */

$('#predChannels').innerHTML = '<span class="k">Candidate channels</span>' + CHANNELS.map(c =>
  `<label class="pick"><input type="checkbox" value="${c}" checked> ${c}</label>`).join('');

$('#predScope').onchange = () => {
  const s = $('#predScope').value;
  document.querySelectorAll('.scope-customer').forEach(el => el.classList.toggle('hidden', s !== 'customer'));
  document.querySelectorAll('.scope-audience').forEach(el => el.classList.toggle('hidden', s !== 'audience'));
};

const confClass = c => (c === 'high' ? 'ok' : c === 'medium' ? 'warn' : 'bad');

$('#predSubmit').onclick = () => busy('predict', async () => {
  const body = {
    objective: $('#predObjective').value,
    channels: [...document.querySelectorAll('#predChannels input:checked')].map(i => i.value),
  };
  if (!body.channels.length) throw new Error('Pick at least one channel');
  const lookback = parseInt($('#predLookback').value, 10);
  if (lookback) body.lookback_days = lookback;
  const scope = $('#predScope').value;
  if (scope === 'customer') body.customer_id = $('#predCustomer').value.trim();
  if (scope === 'audience') {
    body.audience = {};
    if ($('#predMinScore').value !== '') body.audience.min_score = parseFloat($('#predMinScore').value);
    if ($('#predDays').value !== '') body.audience.last_active_days = parseInt($('#predDays').value, 10);
  }
  renderPrediction(await api('/predictions/channel', { method: 'POST', body: JSON.stringify(body) }));
});

function renderPrediction(r) {
  const meta = r.meta || {};
  const pctCell = v => `<span class="num">${fmtRate(v)}</span>`;
  const bar = p => `<div class="pbar"><div style="width:${Math.round((p || 0) * 100)}%"></div></div>
    <span class="num">${((p || 0) * 100).toFixed(1)}%</span>`;
  const flags = c => [
    c.low_data ? badge('low data', 'warn') : '',
    (c.reasons || []).some(x => x.startsWith('high opt-out')) ? badge('opt-out risk', 'bad') : '',
    (c.reasons || []).some(x => x.startsWith('high bounce')) ? badge('bounce risk', 'bad') : '',
  ].join(' ');
  const ev = c => {
    const e = c.evidence || {};
    let s = `${fmtNum(e.objective_successes)} / ${fmtNum(e.objective_delivered)} this objective`;
    if (e.target_delivered !== undefined) {
      s += `<br>${fmtNum(e.target_successes)} / ${fmtNum(e.target_delivered)} ${r.scope}`;
    }
    return `<span class="num small">${s}</span>`;
  };
  $('#predResult').innerHTML = `
    <div class="cards">
      <div class="card"><div class="k">recommended channel</div><div class="v">${badge(r.recommended_channel, 'accent')}</div></div>
      <div class="card"><div class="k">confidence</div><div class="v">${badge(r.confidence, confClass(r.confidence))}</div></div>
      <div class="card"><div class="k">success event</div><div class="v">${badge(r.success_event)}</div></div>
      <div class="card"><div class="k">scope</div><div class="v">${badge(r.scope)}${r.customer_id ? ' <span class="mono small">' + esc(r.customer_id) + '</span>' : ''}</div></div>
      ${meta.audience_size !== undefined ? `<div class="card"><div class="k">audience size</div><div class="v num">${fmtNum(meta.audience_size)}</div></div>` : ''}
    </div>
    <p class="summary">${esc(r.advice)}</p>
    ${table(['rank', 'channel', 'predicted rate', '90% range', 'chance best', 'flags', 'evidence (successes / delivered)', 'reasons'],
      (r.channels || []).map(c => [
        `<span class="num">${esc(c.rank)}</span>`,
        badge(c.channel, c.channel === r.recommended_channel ? 'accent' : ''),
        pctCell(c.predicted_rate),
        `<span class="num small">${fmtRate(c.interval_90?.[0])} – ${fmtRate(c.interval_90?.[1])}</span>`,
        bar(c.prob_best),
        flags(c),
        ev(c),
        `<ul class="reasons">${(c.reasons || []).map(x => `<li>${esc(x)}</li>`).join('')}</ul>`,
      ]))}
    <p class="meta num">model: ${esc(meta.model)} · samples: ${fmtNum(meta.samples)} · lookback: ${fmtNum(meta.lookback_days)}d · took_ms: ${fmtNum(meta.took_ms)}</p>
    ${jsonBlock(r)}`;
}

/* ---------- 5. AI ---------- */

const providerBadges = r => `
  <div class="badges">
    ${r.provider ? badge('provider: ' + r.provider) : ''}
    ${badge('fallback_used: ' + (r.fallback_used ? 'true' : 'false'), r.fallback_used ? 'warn' : 'ok')}
  </div>`;

$('#aiAnalyze').onclick = () => busy('ai', async () => {
  const id = encodeURIComponent($('#aiCampId').value.trim());
  const r = await api(`/campaigns/${id}/analyze`, { method: 'POST', body: '{}' });
  const a = r.analysis || {};
  const list = (t, arr) => arr && arr.length
    ? `<h4>${t}</h4><ul>${arr.map(x => `<li>${esc(x)}</li>`).join('')}</ul>` : '';
  $('#aiResult').innerHTML = `
    ${providerBadges(r)}
    ${a.summary ? `<p class="summary">${esc(a.summary)}</p>` : ''}
    ${a.confidence !== undefined ? `<p>confidence: <span class="num">${fmtScore(a.confidence)}</span></p>` : ''}
    ${list('strengths', a.strengths)}
    ${list('weaknesses', a.weaknesses)}
    ${list('anomalies', a.anomalies)}
    ${r.facts ? `<details><summary>facts</summary><pre>${esc(JSON.stringify(r.facts, null, 2))}</pre></details>` : ''}
    ${jsonBlock(r)}`;
});

$('#aiRecommend').onclick = () => busy('ai', async () => {
  const id = encodeURIComponent($('#aiCampId').value.trim());
  const r = await api(`/campaigns/${id}/recommend`, { method: 'POST', body: '{}' });
  $('#aiResult').innerHTML = `
    ${providerBadges(r)}
    ${table(['area', 'suggestion', 'reasoning', 'confidence'], (r.recommendations || []).map(x => [
      badge(x.area), esc(x.suggestion), esc(x.reasoning),
      `<span class="num">${fmtScore(x.confidence)}</span>`,
    ]))}
    ${r.facts ? `<details><summary>facts</summary><pre>${esc(JSON.stringify(r.facts, null, 2))}</pre></details>` : ''}
    ${jsonBlock(r)}`;
});

/* ---------- Logs ---------- */

let logsLoaded = false, logCursor = null;

const LOG_FILTERS = {
  event_id: '#logEvent', customer_id: '#logCustomer', campaign_id: '#logCampaign',
  request_id: '#logRequest', stage: '#logStage', level: '#logLevel',
};

function setLogFilters(f) {
  Object.entries(LOG_FILTERS).forEach(([k, sel]) => { $(sel).value = f[k] || ''; });
}

function logQuery(cursor) {
  const q = new URLSearchParams({ limit: '50' });
  Object.entries(LOG_FILTERS).forEach(([k, sel]) => {
    const v = $(sel).value.trim();
    if (v) q.set(k, v);
  });
  if (cursor) q.set('cursor', cursor);
  return '/logs?' + q.toString();
}

function loadLogs() {
  logsLoaded = true;
  return busy('logs', async () => {
    logCursor = null;
    renderLogs(await api(logQuery()), true);
  });
}

$('#logSearch').onclick = loadLogs;
$('#logClear').onclick = () => { setLogFilters({}); loadLogs(); };
$('#logMore').onclick = () => busy('logs', async () => {
  renderLogs(await api(logQuery(logCursor)), false);
});
document.querySelectorAll('#view-logs input').forEach(i =>
  i.addEventListener('keydown', e => { if (e.key === 'Enter') loadLogs(); }));

const levelClass = l => (l === 'error' ? 'bad' : l === 'warn' ? 'warn' : l === 'info' ? 'ok' : '');
const stageClass = s => ({ processed: 'ok', ingested: '', duplicate: '', retry: 'warn', dead_lettered: 'bad', replayed: 'accent' }[s] || '');

function renderLogs(r, reset) {
  const rows = (r.data || []).map(e => {
    const d = isObj(e.details) ? e.details : {};
    const detail = d.error
      ? `<div class="err-cell small">${esc(d.error)}</div>`
      : Object.keys(d).length
        ? `<div class="mono small muted">${esc(Object.entries(d).map(([k, v]) => k + '=' + v).join(' '))}</div>` : '';
    return `<tr>
      <td class="small">${fmtTime(e.at)}</td>
      <td>${badge(e.level, levelClass(e.level))}</td>
      <td>${badge(e.stage, stageClass(e.stage))}</td>
      <td>${e.event_id ? `<a href="#" class="mono ev-link" data-ev="${esc(e.event_id)}">${esc(e.event_id)}</a>` : '—'}</td>
      <td class="mono small">${esc(e.customer_id ?? '—')}</td>
      <td class="mono small">${esc(e.campaign_id ?? '—')}</td>
      <td class="num">${esc(e.attempt ?? '')}</td>
      <td>${esc(e.message)}${detail}</td>
      <td class="mono small">${esc(e.request_id ?? '')}${e.worker ? `<div class="muted">${esc(e.worker)}</div>` : ''}</td>
    </tr>`;
  });
  if (reset || !$('#logTable')) {
    $('#logResult').innerHTML = `<table id="logTable"><thead><tr>
      <th>time</th><th>level</th><th>stage</th><th>event_id</th><th>customer</th><th>campaign</th>
      <th>attempt</th><th>message</th><th>request / worker</th>
    </tr></thead><tbody></tbody></table>`;
  }
  $('#logTable tbody').insertAdjacentHTML('beforeend', rows.join(''));
  if (reset && !(r.data || []).length) {
    $('#logResult').innerHTML = '<p class="meta">No log entries match these filters. Ingest an event, or clear the filters. (Logging can be off: EVENT_LOG_MODE=off, or errors-only.)</p>';
  }
  logCursor = r.next_cursor || null;
  $('#logMore').classList.toggle('hidden', !r.has_more);
}

$('#logResult').addEventListener('click', e => {
  const a = e.target.closest('a.ev-link');
  if (!a) return;
  e.preventDefault();
  setLogFilters({ event_id: a.dataset.ev });
  loadLogs();
});

/* ---------- 6. System ---------- */

let sysLoaded = false, dlqCursor = null;

function loadSystem() {
  sysLoaded = true;
  return busy('system', async () => {
    const res = await fetch(base() + '/system/health'); // 503 body is still valid
    const h = await res.json().catch(() => null);
    if (!h) throw new Error(`HTTP ${res.status} ${res.statusText}`);
    renderHealth(h);
  }).then(() => busy('system', async () => {
    dlqCursor = null;
    renderDlq(await api('/system/dlq?limit=25'), true);
  }));
}

$('#sysRefresh').onclick = loadSystem;

$('#dlqMore').onclick = () => busy('system', async () => {
  renderDlq(await api(`/system/dlq?limit=25&cursor=${encodeURIComponent(dlqCursor)}`), false);
});

function renderHealth(h) {
  $('#sysHealth').innerHTML = `
    <div class="cards">
      <div class="card"><div class="k">status</div><div class="v">${statusBadge(h.status)}</div></div>
      <div class="card"><div class="k">postgres</div><div class="v">${statusBadge(h.postgres)}</div></div>
      <div class="card"><div class="k">redis</div><div class="v">${statusBadge(h.redis)}</div></div>
      <div class="card"><div class="k">queue_depth</div><div class="v num">${fmtNum(h.queue_depth)}</div></div>
      <div class="card"><div class="k">dlq_size</div><div class="v num">${fmtNum(h.dlq_size)}</div></div>
      ${h.version ? `<div class="card"><div class="k">version</div><div class="v mono small">${esc(h.version.slice(0, 7))}</div></div>` : ''}
    </div>
    ${jsonBlock(h)}`;
}

function renderDlq(r, reset) {
  const rows = (r.data || []).map(e => `<tr>
    <td class="num">${esc(e.id)}</td>
    <td class="mono">${esc(e.event_id ?? '')}</td>
    <td class="err-cell">${esc(e.error ?? '')}</td>
    <td class="num">${esc(e.attempts ?? '')}</td>
    <td>${fmtTime(e.failed_at)}</td>
    <td>${e.replayed_at ? fmtTime(e.replayed_at) : '—'}</td>
    <td><button class="replay" data-id="${esc(e.id)}">replay</button></td>
  </tr>`);
  if (reset || !$('#dlqTable')) {
    $('#sysDlq').innerHTML = `<table id="dlqTable"><thead><tr>
      <th>id</th><th>event_id</th><th>error</th><th>attempts</th><th>failed_at</th><th>replayed_at</th><th></th>
    </tr></thead><tbody></tbody></table>`;
  }
  $('#dlqTable tbody').insertAdjacentHTML('beforeend', rows.join(''));
  dlqCursor = r.next_cursor || null;
  $('#dlqMore').classList.toggle('hidden', !r.has_more);
}

$('#sysDlq').addEventListener('click', e => {
  const b = e.target.closest('button.replay');
  if (!b) return;
  busy('system', async () => {
    await api(`/system/dlq/${encodeURIComponent(b.dataset.id)}/replay`, { method: 'POST', body: '{}' });
    renderDlq(await api('/system/dlq?limit=25'), true);
  });
});
