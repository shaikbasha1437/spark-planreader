/* Spark PlanReader views: summary, findings, collapsible tree, pan/zoom graph, node details, glossary tooltips. */
(function (root) {
  'use strict';
  const SP = root.SP; const { h } = SP.ui; const P = SP.parser; const T = SP.text; const G = SP.glossary;
  const SEV_LABEL = { high: 'High', medium: 'Medium', low: 'Low', info: 'Info' };
  const NS = 'http://www.w3.org/2000/svg';
  function s(tag, attrs, ...kids) { const el = document.createElementNS(NS, tag); Object.entries(attrs || {}).forEach(([k, v]) => { if (v == null || v === false) return; if (k === 'text') el.textContent = v; else if (k.startsWith('on') && typeof v === 'function') el.addEventListener(k.slice(2), v); else el.setAttribute(k, v === true ? '' : v); }); kids.flat().forEach((c) => c && el.append(c)); return el; }
  const cgClass = (n) => (n.codegen ? 'cg' + (n.codegen % 8) : '');

  // ---------- metrics ----------
  function skewOf(n) {
    const m = n.metrics; if (!m) return null;
    const x = (P.isExchange(n) && m['records read']) || m['number of output rows'];
    if (!x || !x.tasks || !(x.tasks.count > 1 || x.tasks.count == null) || !x.tasks.median || x.tasks.max < 1000) return null;
    const r = x.tasks.max / x.tasks.median; return r >= 2 ? r : null;
  }
  function metricChips(n) {
    const out = []; const m = n.metrics;
    if (m) {
      const rows = m['number of output rows'] || (P.isExchange(n) ? m['records read'] : null); if (rows) out.push({ k: 'rows', t: T.fmtNum(rows.value) + ' rows' });
      const size = m['data size'] || m['size of files read'] || m['shuffle bytes written']; if (size) out.push({ k: 'size', t: T.fmtBytes(size.value) });
      const sp = m['spill size']; if (sp && sp.value > 0) out.push({ k: 'spill', t: 'spill ' + T.fmtBytes(sp.value), bad: true });
      const tm = m['duration'] || m['scan time'] || m['time in aggregation build'] || m['sort time']; if (tm && tm.value) out.push({ k: 'time', t: T.fmtMs(tm.value) });
      const sk = skewOf(n); if (sk) out.push({ k: 'skew', t: 'skew ' + (sk >= 10 ? Math.round(sk) : sk.toFixed(1)) + '×', bad: sk >= 4 });
      const files = m['number of files read']; if (files) out.push({ k: 'files', t: T.fmtNum(files.value) + ' files' });
    } else if (n.stats) {
      if (n.stats.rowCount != null) out.push({ k: 'rows', t: T.fmtNum(n.stats.rowCount) + ' rows' });
      if (n.stats.sizeInBytes != null) out.push({ k: 'size', t: T.fmtBytes(n.stats.sizeInBytes) });
    } else if (n._costSize != null) out.push({ k: 'size', t: '~' + T.fmtBytes(n._costSize) });
    return out;
  }
  const chipEls = (n) => metricChips(n).map((c) => h('span', { class: 'chip m-' + c.k + (c.bad ? ' bad' : ''), text: c.t }));
  // ---------- tooltip ----------
  const tip = h('div', { class: 'tip', role: 'tooltip', id: 'gloss-tip', hidden: true });
  document.addEventListener('DOMContentLoaded', () => document.body.append(tip));
  let tipFor = null;
  function showTip(el) {
    const name = el.getAttribute('data-gloss'); const text = G.lookup(name); if (!text) return;
    tip.replaceChildren(h('strong', { text: name }), h('span', { text: ' ' + text })); tip.hidden = false; tipFor = el;
    const r = el.getBoundingClientRect(); const tw = Math.min(360, window.innerWidth - 16);
    tip.style.maxWidth = tw + 'px'; const left = Math.max(8, Math.min(window.innerWidth - tw - 8, r.left));
    tip.style.left = left + 'px'; const below = r.bottom + 8; tip.style.top = (below + tip.offsetHeight > window.innerHeight ? Math.max(8, r.top - tip.offsetHeight - 8) : below) + 'px';
  }
  function hideTip(el) { if (!el || el === tipFor) { tip.hidden = true; tipFor = null; } }
  ['mouseover', 'focusin'].forEach((ev) => document.addEventListener(ev, (e) => { const el = e.target.closest && e.target.closest('[data-gloss]'); if (el) showTip(el); }));
  ['mouseout', 'focusout'].forEach((ev) => document.addEventListener(ev, (e) => { const el = e.target.closest && e.target.closest('[data-gloss]'); if (el && (!e.relatedTarget || !el.contains(e.relatedTarget))) hideTip(el); }));
  document.addEventListener('keydown', (e) => { if (e.key === 'Escape' && !tip.hidden) { hideTip(); } }, true);
  document.addEventListener('scroll', () => hideTip(), true);

  // ---------- summary ----------
  function summary(res, d, extra) {
    const sm = d.summary; const score = sm.score;
    const facts = [
      ['Input', res.input === 'eventlog' ? 'Event log' : 'EXPLAIN ' + res.format],
      ['Spark', res.version || 'unknown'],
      ['AQE', res.aqe.present ? (res.aqe.final ? 'final plan' : 'initial plan') : 'off / not shown'],
      ['Shuffles', String(sm.shuffles)], ['Broadcasts', String(sm.broadcasts)],
      ['Joins', Object.entries(sm.joins).map(([k, v]) => v + '× ' + k.replace(/Join$/, '')).join(', ') || 'none'],
      ['Operators', String(res.nodes.length)]
    ];
    if (res.execution && res.execution.duration != null) facts.splice(2, 0, ['Duration', T.fmtMs(res.execution.duration)]);
    const ring = h('div', { class: 'score ' + (score >= 85 ? 'good' : score >= 60 ? 'ok' : 'bad'), role: 'img', 'aria-label': 'Health score ' + score + ' out of 100' }, h('b', { text: String(score) }), h('small', { text: '/100' }));
    const counts = h('div', { class: 'sevcounts' }, ['high', 'medium', 'low'].map((k) => h('span', { class: 'sev sev-' + k, text: sm[k] + ' ' + SEV_LABEL[k].toLowerCase() })));
    return h('section', { class: 'summary', 'aria-label': 'Summary' }, ring, h('div', { class: 'sumbody' }, counts, h('dl', { class: 'facts' }, facts.map(([k, v]) => h('div', null, h('dt', { text: k }), h('dd', { text: v })))), extra || null));
  }
  function notices(res) {
    if (!res.notices.length) return null;
    return h('div', { class: 'notices' }, res.notices.map((n) => h('p', { class: 'notice ' + n.level, role: n.level === 'error' ? 'alert' : null }, h('span', { class: 'ico', 'aria-hidden': 'true', text: n.level === 'error' ? '!' : n.level === 'warn' ? '!' : 'i' }), n.text)));
  }
  // ---------- findings ----------
  function fixBlock(fix, copy) {
    if (!fix) return null;
    const tabs = [['pyspark', 'PySpark'], ['sql', 'SQL hint'], ['config', 'Config']].filter(([k]) => fix[k]);
    if (!tabs.length) return null;
    const id = 'fx' + Math.random().toString(36).slice(2, 8);
    const panels = {}; const btns = {};
    const wrap = h('div', { class: 'fix' });
    const bar = h('div', { class: 'fixtabs', role: 'tablist', 'aria-label': 'Fix' });
    const select = (k, focus) => { tabs.forEach(([t]) => { btns[t].setAttribute('aria-selected', String(t === k)); btns[t].tabIndex = t === k ? 0 : -1; panels[t].hidden = t !== k; }); if (focus) btns[k].focus(); };
    tabs.forEach(([k, label], i) => {
      btns[k] = h('button', { type: 'button', role: 'tab', id: id + k, 'aria-controls': id + k + 'p', text: label, onclick: () => select(k),
        onkeydown: (e) => { const dir = e.key === 'ArrowRight' ? 1 : e.key === 'ArrowLeft' ? -1 : 0; if (dir) { e.preventDefault(); select(tabs[(i + dir + tabs.length) % tabs.length][0], true); } } });
      bar.append(btns[k]);
      panels[k] = h('div', { role: 'tabpanel', id: id + k + 'p', 'aria-labelledby': id + k, class: 'fixpanel' }, h('pre', null, h('code', { text: fix[k] })), h('button', { type: 'button', class: 'btn small copybtn', text: 'Copy', 'aria-label': 'Copy ' + label + ' fix', onclick: () => copy(fix[k], label + ' fix copied') }));
    });
    wrap.append(bar, ...tabs.map(([k]) => panels[k]));
    if (fix.notes) wrap.append(h('p', { class: 'muted small', text: fix.notes }));
    select(tabs[0][0]);
    return wrap;
  }
  function findings(d, opts) {
    const box = h('div', { class: 'findings' });
    if (!d.findings.length) { box.append(h('p', { class: 'empty', text: 'No issues found in this plan.' })); return box; }
    const filters = ['all', 'high', 'medium', 'low', 'info']; let cur = 'all';
    const list = h('ol', { class: 'flist' });
    const bar = h('div', { class: 'fbar' }, h('div', { class: 'seg', role: 'group', 'aria-label': 'Filter by severity' }, filters.map((f) => h('button', { type: 'button', class: 'btn small', 'aria-pressed': String(f === 'all'), 'data-f': f, text: f === 'all' ? 'All ' + d.findings.length : SEV_LABEL[f] + ' ' + d.findings.filter((x) => x.severity === f).length, disabled: f !== 'all' && !d.findings.some((x) => x.severity === f), onclick: (e) => { cur = f; bar.querySelectorAll('[data-f]').forEach((b) => b.setAttribute('aria-pressed', String(b.dataset.f === f))); draw(); } }))),
      h('button', { type: 'button', class: 'btn small', text: 'Copy all fixes', onclick: () => opts.copy(allFixes(d), 'All fixes copied') }));
    const draw = () => {
      list.replaceChildren(...d.findings.filter((f) => cur === 'all' || f.severity === cur).map((f) => h('li', { class: 'finding sev-' + f.severity, id: 'f-' + f.id },
        h('div', { class: 'fhead' }, h('span', { class: 'sev sev-' + f.severity, text: SEV_LABEL[f.severity] }), h('h3', { text: f.title })),
        h('p', { text: f.explain }), h('p', { class: 'why' }, h('strong', { text: 'Why it matters: ' }), f.why),
        f.est ? h('p', { class: 'muted small', text: 'Size estimate: ' + T.fmtBytes(f.est.bytes) + ' (from ' + f.est.source + ')' }) : null,
        fixBlock(f.fix, opts.copy),
        f.nodes && f.nodes.length ? h('div', { class: 'factions' }, h('button', { type: 'button', class: 'btn small', text: 'Show in graph', onclick: () => opts.show(f.nodes, 'graph') }), h('button', { type: 'button', class: 'btn small ghost', text: 'Show in tree', onclick: () => opts.show(f.nodes, 'tree') })) : null)));
    };
    draw(); box.append(bar, list); return box;
  }
  function allFixes(d) {
    return d.findings.filter((f) => f.fix).map((f, i) => '# ' + (i + 1) + '. [' + SEV_LABEL[f.severity] + '] ' + f.title + '\n' + (f.fix.pyspark ? '# PySpark\n' + f.fix.pyspark + '\n' : '') + (f.fix.sql ? '# SQL\n' + f.fix.sql.split('\n').map((l) => '# ' + l).join('\n') + '\n' : '') + (f.fix.config ? '# Config\n' + f.fix.config.split('\n').map((l) => (l.startsWith('#') ? l : '# ' + l)).join('\n') + '\n' : '')).join('\n');
  }
  // ---------- search ----------
  function matches(n, q) { if (!q) return false; const t = (n.name + ' ' + P.shortLabel(n) + ' ' + n.text + ' ' + (n.details || []).map((d) => d.join(' ')).join(' ')).toLowerCase(); return q.toLowerCase().split(/\s+/).filter(Boolean).every((w) => t.includes(w)); }
  // ---------- tree ----------
  function tree(res, opts) {
    const q = opts.search || ''; const flagged = opts.flagged || new Set();
    const rootEl = h('ul', { class: 'tree', role: 'tree', 'aria-label': 'Physical plan tree' });
    const rows = [];
    const hitIds = new Set(res.nodes.filter((n) => matches(n, q)).map((n) => n.id));
    const hasHitBelow = (n) => { let f = false; P.walk(n, (x) => { if (hitIds.has(x.id)) f = true; }); return f; };
    const item = (n, depth) => {
      const kids = n.children.filter((c) => c.category !== 'marker' || c.children.length);
      const collapsed = opts.collapsed.has(n.id) && !(q && kids.some(hasHitBelow));
      const label = P.shortLabel(n);
      const row = h('div', { class: 'trow cat-' + n.category + (hitIds.has(n.id) ? ' hit' : q ? ' dim' : '') + (flagged.has(n.id) ? ' flagged' : '') + (opts.selected === n.id ? ' sel' : ''), 'data-id': n.id },
        kids.length ? h('button', { type: 'button', class: 'twisty', tabIndex: -1, 'aria-hidden': 'true', text: collapsed ? '▸' : '▾', onclick: (e) => { e.stopPropagation(); opts.toggle(n.id); } }) : h('span', { class: 'twisty leaf', 'aria-hidden': 'true' }),
        n.codegen ? h('span', { class: 'cgbadge ' + cgClass(n), title: 'Whole-stage codegen ' + n.codegen, text: '*' + n.codegen }) : null,
        h('span', { class: 'tname', 'data-gloss': n.name, text: n.name }),
        label ? h('span', { class: 'tlabel', text: T.stripIds(label) }) : null,
        flagged.has(n.id) ? h('span', { class: 'flag', title: 'Has findings', 'aria-label': 'has findings', text: '⚑' }) : null,
        opts.overlay ? h('span', { class: 'tchips' }, chipEls(n)) : null,
        h('span', { class: 'tstage', title: 'Stage (between exchanges)', text: 'S' + n.stage }));
      const li = h('li', { role: 'treeitem', 'aria-level': String(depth + 1), 'aria-expanded': kids.length ? String(!collapsed) : null, 'aria-selected': String(opts.selected === n.id), tabIndex: -1, 'data-id': n.id, 'aria-label': n.name + (label ? ', ' + T.stripIds(label) : '') + ', stage ' + n.stage }, row);
      rows.push(li);
      if (kids.length && !collapsed) li.append(h('ul', { role: 'group' }, kids.map((c) => item(c, depth + 1))));
      return li;
    };
    if (res.physical) rootEl.append(item(res.physical, 0));
    (res.subqueries || []).forEach((sq) => rootEl.append(item(sq, 0)));
    if (rows.length) { const start = rows.find((r) => r.dataset.id === opts.selected) || rows[0]; start.tabIndex = 0; }
    rootEl.addEventListener('click', (e) => { const li = e.target.closest('li[role=treeitem]'); if (!li || e.target.closest('.twisty')) return; rows.forEach((r) => { r.tabIndex = -1; }); li.tabIndex = 0; opts.select(li.dataset.id); });
    rootEl.addEventListener('keydown', (e) => {
      const li = e.target.closest('li[role=treeitem]'); if (!li) return;
      const vis = [...rootEl.querySelectorAll('li[role=treeitem]')]; const i = vis.indexOf(li); let next = null;
      if (e.key === 'ArrowDown') next = vis[i + 1]; else if (e.key === 'ArrowUp') next = vis[i - 1]; else if (e.key === 'Home') next = vis[0]; else if (e.key === 'End') next = vis[vis.length - 1];
      else if (e.key === 'ArrowRight') { if (li.getAttribute('aria-expanded') === 'false') { opts.toggle(li.dataset.id, true); e.preventDefault(); return; } next = li.querySelector('li[role=treeitem]'); }
      else if (e.key === 'ArrowLeft') { if (li.getAttribute('aria-expanded') === 'true') { opts.toggle(li.dataset.id, true); e.preventDefault(); return; } next = li.parentElement.closest('li[role=treeitem]'); }
      else if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); opts.select(li.dataset.id); return; }
      if (next) { e.preventDefault(); li.tabIndex = -1; next.tabIndex = 0; next.focus(); }
    });
    return { el: rootEl, hits: [...hitIds] };
  }
  // ---------- graph ----------
  function trunc(t, n) { t = String(t || ''); return t.length > n ? t.slice(0, n - 1) + '…' : t; }
  function graph(res, opts) {
    const L = SP.layout.layout(res.physical, { hidePlumbing: opts.hidePlumbing });
    const q = opts.search || ''; const flagged = opts.flagged || new Set(); const focusSet = new Set(opts.focus || []);
    const wrap = h('div', { class: 'graphwrap' });
    const svg = s('svg', { class: 'graph', role: 'group', 'aria-label': 'Plan graph. Drag to pan, scroll or use plus and minus to zoom, Tab to move between operators.', tabindex: '0' });
    const defs = s('defs', null, s('marker', { id: 'arr', viewBox: '0 0 10 10', refX: '9', refY: '5', markerWidth: '7', markerHeight: '7', orient: 'auto-start-reverse' }, s('path', { d: 'M0,0 L10,5 L0,10 z', class: 'arrowhead' })));
    const g = s('g', { class: 'viewport' });
    svg.append(defs, g);
    const PAD = 24; const byId = {}; L.nodes.forEach((o) => { byId[o.id] = o; });
    // stage boxes
    L.stages.forEach((b) => g.append(s('rect', { class: 'stagebox st' + (b.stage % 6), x: b.x0 - 8 + PAD, y: b.y0 - 8 + PAD, width: b.x1 - b.x0 + 16, height: b.y1 - b.y0 + 16, rx: 10 }), s('text', { class: 'stagelabel', x: b.x0 - 4 + PAD, y: b.y0 - 12 + PAD, text: 'Stage ' + b.stage })));
    // edges
    L.edges.forEach((e) => { const a = byId[e.from], b = byId[e.to]; if (!a || !b) return;
      if (e.kind === 'reuse') { const x1 = a.x + a.w + PAD, y1 = a.y + a.h / 2 + PAD, x2 = b.x + b.w + PAD, y2 = b.y + b.h / 2 + PAD; const cx = Math.max(x1, x2) + 60; g.append(s('path', { class: 'edge reuse', d: `M${x1},${y1} C${cx},${y1} ${cx},${y2} ${x2},${y2}`, 'marker-end': 'url(#arr)' })); return; }
      const x1 = a.x + a.w / 2 + PAD, y1 = a.y + PAD, x2 = b.x + b.w / 2 + PAD, y2 = b.y + b.h + PAD; const my = (y1 + y2) / 2;
      g.append(s('path', { class: 'edge' + (e.boundary ? ' boundary' : ''), d: `M${x1},${y1} C${x1},${my} ${x2},${my} ${x2},${y2}`, 'marker-end': 'url(#arr)' })); });
    // nodes
    const nodeEls = {};
    L.nodes.forEach((o) => {
      const n = o.node; const chips = opts.overlay ? metricChips(n) : []; const label = T.stripIds(P.shortLabel(n));
      const cls = ['gnode', 'cat-' + n.category, cgClass(n), q ? (matches(n, q) ? 'hit' : 'dim') : '', flagged.has(n.id) ? 'flagged' : '', focusSet.has(n.id) ? 'focus' : '', opts.selected === n.id ? 'sel' : ''].filter(Boolean).join(' ');
      const el = s('g', { class: cls, transform: `translate(${o.x + PAD},${o.y + PAD})`, tabindex: '0', role: 'button', 'data-id': n.id, 'data-gloss': n.name, 'aria-label': n.name + (label ? ', ' + label : '') + (chips.length ? ', ' + chips.map((c) => c.t).join(', ') : '') + (flagged.has(n.id) ? ', has findings' : '') + '. Press Enter for details.' },
        s('rect', { class: 'body', width: o.w, height: o.h, rx: P.isExchange(n) || n.category === 'broadcast' ? 18 : 6 }),
        n.codegen ? s('rect', { class: 'cgstripe', width: 5, height: o.h, rx: 2 }) : null,
        s('text', { class: 'gname', x: 12, y: 19, text: trunc((n.codegen ? '*(' + n.codegen + ') ' : '') + n.name, 27) }),
        s('text', { class: 'glabel', x: 12, y: 35, text: trunc(label, 30) }),
        chips.length ? s('text', { class: 'gchips', x: 12, y: 51 }, chips.slice(0, 3).map((c, i) => s('tspan', { class: c.bad ? 'bad' : '', text: (i ? ' · ' : '') + c.t }))) : null,
        flagged.has(n.id) ? s('text', { class: 'gflag', x: o.w - 16, y: 18, text: '⚑' }) : null);
      el.addEventListener('click', (ev) => { if (moved) return; opts.select(n.id); });
      el.addEventListener('keydown', (ev) => { if (ev.key === 'Enter' || ev.key === ' ') { ev.preventDefault(); opts.select(n.id); } });
      el.addEventListener('focus', () => ensureVisible(o));
      nodeEls[n.id] = el; g.append(el);
    });
    // pan / zoom
    const W = L.width + PAD * 2 + 80, H = L.height + PAD * 2;
    let view = { x: 0, y: 0, k: 1 };
    const apply = () => { g.setAttribute('transform', `translate(${view.x},${view.y}) scale(${view.k})`); zoomLabel.textContent = Math.round(view.k * 100) + '%'; };
    const size = () => { const r = svg.getBoundingClientRect(); return { w: r.width || 800, h: r.height || 500 }; };
    function fit() { const { w, h: hh } = size(); const k = Math.max(0.15, Math.min(1.2, Math.min(w / W, hh / H))); view = { k, x: (w - W * k) / 2, y: Math.max(0, (hh - H * k) / 2) }; if (H * k > hh) view.y = 8; apply(); }
    function zoomAt(f, cx, cy) { const k = Math.max(0.1, Math.min(3, view.k * f)); const r = k / view.k; view.x = cx - (cx - view.x) * r; view.y = cy - (cy - view.y) * r; view.k = k; apply(); }
    function center(id, z) { const o = byId[id]; if (!o) return; const { w, h: hh } = size(); if (z) view.k = Math.max(view.k, 0.8); view.x = w / 2 - (o.x + PAD + o.w / 2) * view.k; view.y = hh / 2 - (o.y + PAD + o.h / 2) * view.k; apply(); }
    function ensureVisible(o) { const { w, h: hh } = size(); const x = (o.x + PAD) * view.k + view.x, y = (o.y + PAD) * view.k + view.y; if (x < 0 || y < 0 || x + o.w * view.k > w || y + o.h * view.k > hh) center(o.id); }
    let drag = null, moved = false; const pts = new Map(); let pinch = null;
    svg.addEventListener('pointerdown', (e) => { pts.set(e.pointerId, { x: e.clientX, y: e.clientY }); if (pts.size === 2) { const [a, b] = [...pts.values()]; pinch = { d: Math.hypot(a.x - b.x, a.y - b.y), k: view.k }; drag = null; return; } drag = { x: e.clientX, y: e.clientY, vx: view.x, vy: view.y }; moved = false; });
    svg.addEventListener('pointermove', (e) => {
      if (!pts.has(e.pointerId)) return; pts.set(e.pointerId, { x: e.clientX, y: e.clientY });
      if (pinch && pts.size === 2) { const [a, b] = [...pts.values()]; const d = Math.hypot(a.x - b.x, a.y - b.y); const r = svg.getBoundingClientRect(); zoomAt((pinch.k * d / pinch.d) / view.k, (a.x + b.x) / 2 - r.left, (a.y + b.y) / 2 - r.top); moved = true; return; }
      if (!drag) return; const dx = e.clientX - drag.x, dy = e.clientY - drag.y; if (!moved && Math.hypot(dx, dy) < 4) return;
      if (!moved) { moved = true; try { svg.setPointerCapture(e.pointerId); } catch (x) { /* ignore */ } svg.classList.add('panning'); }
      view.x = drag.vx + dx; view.y = drag.vy + dy; apply(); });
    const up = (e) => { pts.delete(e.pointerId); if (pts.size < 2) pinch = null; drag = null; svg.classList.remove('panning'); setTimeout(() => { moved = false; }, 0); };
    svg.addEventListener('pointerup', up); svg.addEventListener('pointercancel', up);
    svg.addEventListener('wheel', (e) => { e.preventDefault(); const r = svg.getBoundingClientRect(); zoomAt(Math.exp(-e.deltaY * (e.ctrlKey ? 0.01 : 0.0015)), e.clientX - r.left, e.clientY - r.top); }, { passive: false });
    svg.addEventListener('keydown', (e) => {
      if (e.target !== svg) return; const { w, h: hh } = size(); const step = 60;
      if (e.key === '+' || e.key === '=') zoomAt(1.2, w / 2, hh / 2); else if (e.key === '-') zoomAt(1 / 1.2, w / 2, hh / 2); else if (e.key === '0') fit();
      else if (e.key === 'ArrowLeft') { view.x += step; apply(); } else if (e.key === 'ArrowRight') { view.x -= step; apply(); } else if (e.key === 'ArrowUp') { view.y += step; apply(); } else if (e.key === 'ArrowDown') { view.y -= step; apply(); } else return;
      e.preventDefault(); });
    const zoomLabel = h('span', { class: 'zoomlabel', 'aria-live': 'polite', text: '100%' });
    const tools = h('div', { class: 'gtools', role: 'toolbar', 'aria-label': 'Graph zoom' },
      h('button', { type: 'button', class: 'btn small icon-btn', 'aria-label': 'Zoom in', text: '+', onclick: () => { const { w, h: hh } = size(); zoomAt(1.25, w / 2, hh / 2); } }),
      h('button', { type: 'button', class: 'btn small icon-btn', 'aria-label': 'Zoom out', text: '−', onclick: () => { const { w, h: hh } = size(); zoomAt(0.8, w / 2, hh / 2); } }),
      h('button', { type: 'button', class: 'btn small', text: 'Fit', onclick: fit }), zoomLabel);
    const legend = h('div', { class: 'glegend', 'aria-hidden': 'true' }, [['exchange', 'Shuffle'], ['broadcast', 'Broadcast'], ['join', 'Join'], ['scan', 'Scan'], ['udf', 'Python'], ['agg', 'Aggregate']].map(([c, t]) => h('span', { class: 'lg cat-' + c }, h('i'), t)), h('span', { class: 'lg' }, h('i', { class: 'dash' }), 'Reused'));
    wrap.append(tools, legend, svg);
    const api = { el: wrap, fit, center, focusNode: (id) => { const el = nodeEls[id]; if (el) { center(id, true); el.focus({ preventScroll: true }); } }, nodeEls, hits: L.nodes.filter((o) => q && matches(o.node, q)).map((o) => o.id), hiddenIds: res.nodes.filter((n) => !byId[n.id]).map((n) => n.id) };
    requestAnimationFrame(() => { if (opts.focus && opts.focus.length && byId[opts.focus[0]]) center(opts.focus[0], true); else fit(); });
    return api;
  }
  // ---------- node details ----------
  function detailBody(n, res, d, opts) {
    const a = n.attrs; const rows = [];
    const add = (k, v) => { if (v == null || v === '' || (Array.isArray(v) && !v.length && k !== 'Partition filters' && k !== 'Pushed filters')) return; rows.push([k, Array.isArray(v) ? (v.length ? v.join(', ') : '[] (none)') : String(v)]); };
    add('Category', n.category); add('Stage', 'S' + n.stage + ' (stage boundaries are at exchanges)'); if (n.codegen) add('Codegen', 'Whole-stage codegen block ' + n.codegen); if (n.opId != null) add('Operator id', '(' + n.opId + ')');
    if (n.category === 'scan') { add('Table / path', a.table || (a.location && a.location.paths ? a.location.paths.join(', ') : null)); add('Format', a.format); add('Columns read', a.readCols ? a.readCols.length + (a.readSchemaTruncated ? '+' : '') + ': ' + a.readCols.join(', ') : null); const pp = P.scanParts(n); add('Partition columns', pp.partCols.length ? pp.partCols.join(', ') : null); add('Partition filters', a.partitionFilters); add('Pushed filters', a.pushedFilters); add('Data filters', a.dataFilters); add('Runtime filters', a.runtimeFilters); }
    if (a.partitioning) add('Partitioning', a.partitioning.type + (a.partitioning.keys && a.partitioning.keys.length ? ' by ' + a.partitioning.keys.map(T.attrName).join(', ') : '') + (a.partitioning.num != null ? ', ' + a.partitioning.num + ' partitions' : ''));
    add('Shuffle origin', a.shuffleOrigin); add('Join type', a.joinType); add('Build side', a.buildSide); add('Left keys', a.leftKeys && a.leftKeys.map(T.attrName)); add('Right keys', a.rightKeys && a.rightKeys.map(T.attrName)); add('Condition', a.condition && T.stripIds(a.condition)); add('Skew split', a.skew === true ? 'yes (AQE)' : null);
    add('Group by', a.keys && (a.keys.length ? a.keys.map(T.attrName) : ['(global)'])); add('Functions', a.functions && a.functions.map(T.stripIds)); add('Sort order', a.sortOrder && a.sortOrder.map(T.stripIds)); add('Global sort', a.global != null ? String(a.global) : null);
    add('Partition by', a.partitionSpec ? (a.partitionSpec.length ? a.partitionSpec.map(T.attrName) : ['(none)']) : null); add('Order by', a.orderSpec && a.orderSpec.map(T.stripIds)); add('Window functions', a.windowExprs && a.windowExprs.map(T.stripIds));
    add('Python UDFs', a.udfs && a.udfs.map(T.stripIds)); add('Projections', a.projections); add('Generator', a.generator && T.stripIds(a.generator)); add('Limit', a.num != null ? a.num : a.limit); add('Read mode', a.readMode); add('Query stage', a.stageId); add('Storage level', a.storageLevel);
    if (n.stats) add('Statistics', (n.stats.sizeInBytes != null ? T.fmtBytes(n.stats.sizeInBytes) : '') + (n.stats.rowCount != null ? ', ' + T.fmtNum(n.stats.rowCount) + ' rows' : ''));
    if (n.reuseOf) { const o = res.nodes.find((x) => x.id === n.reuseOf); add('Reuses', o ? o.name + ' ' + T.stripIds(P.shortLabel(o)) : n.reuseOf); }
    const gl = G.lookup(n.name);
    const myFindings = d.findings.filter((f) => (f.nodes || []).includes(n.id));
    const body = h('div', { class: 'detail' },
      gl ? h('p', { class: 'gloss' }, gl) : h('p', { class: 'muted', text: 'No glossary entry for this operator yet.' }),
      myFindings.length ? h('div', { class: 'dfind' }, h('h3', { text: 'Findings here' }), h('ul', null, myFindings.map((f) => h('li', null, h('span', { class: 'sev sev-' + f.severity, text: SEV_LABEL[f.severity] }), ' ', h('a', { href: '#f-' + f.id, text: f.title, onclick: (e) => { e.preventDefault(); opts.gotoFinding(f.id); } }))))) : null,
      rows.length ? h('dl', { class: 'kv' }, rows.map(([k, v]) => h('div', null, h('dt', { text: k }), h('dd', { text: v })))) : null,
      n.metrics ? metricsTable(n.metrics) : null,
      n.details && n.details.length ? h('details', null, h('summary', { text: 'FORMATTED details' }), h('pre', { class: 'raw', text: n.details.map(([k, v]) => k + ': ' + v).join('\n') })) : null,
      h('details', { open: !n.details || !n.details.length ? null : null }, h('summary', { text: 'Raw plan line' }), h('pre', { class: 'raw', text: (n.codegen ? '*(' + n.codegen + ') ' : '') + n.text })));
    return body;
  }
  function metricsTable(m) {
    const fmt = (v, t) => (v == null ? '' : t === 'size' ? T.fmtBytes(v) : t === 'timing' || t === 'nsTiming' ? T.fmtMs(v) : T.fmtNum(v));
    const keys = Object.keys(m); const anyTasks = keys.some((k) => m[k].tasks);
    return h('div', { class: 'mtable' }, h('h3', { text: 'SQL metrics' }), h('div', { class: 'tablewrap' }, h('table', null,
      h('thead', null, h('tr', null, h('th', { scope: 'col', text: 'Metric' }), h('th', { scope: 'col', text: 'Total' }), anyTasks ? [h('th', { scope: 'col', text: 'Min task' }), h('th', { scope: 'col', text: 'Median' }), h('th', { scope: 'col', text: 'Max task' }), h('th', { scope: 'col', text: 'Tasks' })] : null)),
      h('tbody', null, keys.map((k) => { const x = m[k]; const t = x.tasks; return h('tr', null, h('th', { scope: 'row', text: k }), h('td', { text: x.text || fmt(x.value, x.type) }), anyTasks ? [h('td', { text: t ? fmt(t.min, x.type) : '' }), h('td', { text: t ? fmt(t.median, x.type) : '' }), h('td', { class: t && t.median && t.max / t.median >= 4 && t.max > 1000 ? 'bad' : '', text: t ? fmt(t.max, x.type) : '' }), h('td', { text: t && t.count != null ? String(t.count) : '' })] : null); })))));
  }
  SP.views = { summary, notices, findings, tree, graph, detailBody, metricChips, matches, allFixes, skewOf, svg: s };
})(self);
