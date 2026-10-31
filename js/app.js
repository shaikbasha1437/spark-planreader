/* Spark PlanReader app: state, input, analysis, result views, compare mode and dialogs. */
(function (root) {
  'use strict';
  const SP = root.SP; const { h, toast, openModal, download, copy, stack } = SP.ui;
  const P = SP.parser, E = SP.eventlog, D = SP.diagnose, X = SP.diff, R = SP.report, V = SP.views, T = SP.text;
  const store = SP.store.create();
  const $ = (id) => document.getElementById(id);
  const MAX_HISTORY_TEXT = 400 * 1024;
  const SHARE_LIMIT = 60000; // characters of URL hash; most browsers and chat apps cope with this
  const S = { mode: 'analyze', text: '', file: null, res: null, diag: null, ev: null, execId: null, view: 'findings', search: '', overlay: true, hidePlumbing: true, showInitial: false, selected: null, focus: [], collapsed: new Set(), title: '', historyId: null, cmp: null, cmpFiles: { a: null, b: null } };
  const pref = store.load() || {}; if (pref.overlay === false) S.overlay = false; if (pref.hidePlumbing === false) S.hidePlumbing = false; if (pref.view) S.view = pref.view;
  const savePref = () => store.save({ overlay: S.overlay, hidePlumbing: S.hidePlumbing, view: S.view });
  const say = (msg, opts) => toast(msg, opts);
  const doCopy = async (text, msg) => { const ok = await copy(text); say(ok ? (msg || 'Copied') : 'Copy failed: select the text and copy it by hand'); };

  // ---------- analysis ----------
  function runAnalysis(text, opts) {
    opts = opts || {};
    let res, ev = null;
    try {
      if (E.looksLikeEventLog(text)) {
        ev = E.parseEventLog(text);
        const id = opts.execId != null && ev.executions.some((x) => x.id === opts.execId) ? opts.execId : E.defaultExecution(ev);
        res = E.planFor(ev, id); S.execId = id;
        if (!ev.executions.length) res.notices.unshift(...ev.notices.filter((n) => n.level === 'error'));
      } else { res = P.parsePlan(text); S.execId = null; }
    } catch (e) { res = P.finish({ input: 'plan', format: 'simple', sections: [], physical: null, logical: {}, subqueries: [], notices: [{ level: 'error', text: 'Could not read this input: ' + e.message }], aqe: { present: false, final: false } }); }
    return { res, ev, diag: D.diagnose(res) };
  }
  function currentText() { return S.file ? S.file.text : $('planText').value; }
  function analyze(opts) {
    opts = opts || {};
    const text = currentText();
    if (!text.trim()) { say('Paste a plan or open a file first'); $('planText').focus(); return; }
    const r = runAnalysis(text, opts);
    S.text = text; S.res = r.res; S.ev = r.ev; S.diag = r.diag; S.selected = null; S.focus = []; S.collapsed = new Set(); S.showInitial = false; S.search = '';
    if (opts.title) S.title = opts.title; else if (!opts.keepTitle) S.title = autoTitle(r.res, r.ev);
    render();
    if (r.res.ok && opts.save !== false) saveHistory('analyze');
    if (opts.scroll !== false && window.innerWidth < 900) $('results').scrollIntoView({ behavior: 'smooth', block: 'start' });
  }
  function autoTitle(res, ev) {
    if (S.file && S.file.name) return S.file.name + (res.execution ? ' · ' + (res.execution.description || 'execution ' + res.execution.id) : '');
    if (!res.ok) return 'Plan';
    const joins = res.nodes.filter((n) => n.category === 'join').map((n) => n.name.replace(/Join$/, ''));
    const scans = [...new Set(res.nodes.filter((n) => n.category === 'scan').map((n) => D.tableOf(n)).filter(Boolean))];
    return (scans.slice(0, 3).join(', ') || res.nodes[0].name) + (joins.length ? ' · ' + joins[0] + ' join' : '');
  }
  function saveHistory(kind) {
    const e = kind === 'compare' ? { kind, title: S.cmp.title || 'Comparison', a: S.cmp.textA.length < MAX_HISTORY_TEXT ? S.cmp.textA : null, b: S.cmp.textB.length < MAX_HISTORY_TEXT ? S.cmp.textB : null, score: S.cmp.diff.score.after, summary: S.cmp.diff.summary[0] }
      : { kind, title: S.title, text: S.text.length < MAX_HISTORY_TEXT ? S.text : null, fileName: S.file ? S.file.name : null, execId: S.execId, score: S.diag.summary.score, format: S.res.format, version: S.res.version, size: S.text.length };
    // Same input as the newest entry: update it instead of adding a duplicate.
    const top = store.history.list()[0];
    if (top && top.kind === e.kind && ((kind === 'analyze' && top.text != null && top.text === e.text && top.execId === e.execId) || (kind === 'compare' && top.a === e.a && top.b === e.b))) { store.history.update(top.id, { score: e.score }); S.historyId = top.id; return; }
    S.historyId = store.history.add(e).id;
  }

  // ---------- rendering ----------
  const flaggedSet = () => { const s = new Set(); (S.diag ? S.diag.findings : []).forEach((f) => { if (f.severity !== 'info') (f.nodes || []).forEach((id) => s.add(id)); }); return s; };
  function viewRes() {
    if (S.showInitial && S.res.initialPhysical) { const nodes = []; P.walk(S.res.initialPhysical, (n) => nodes.push(n)); return Object.assign({}, S.res, { physical: S.res.initialPhysical, nodes, subqueries: [] }); }
    return S.res;
  }
  let graphApi = null; let treeHits = [];
  function render() {
    const box = $('results'); box.replaceChildren();
    if (S.mode === 'compare') { renderCompare(box); return; }
    if (!S.res) { box.append(welcome()); return; }
    const res = S.res, d = S.diag;
    const head = h('div', { class: 'reshead' }, h('h2', { class: 'restitle', text: S.title || 'Plan' }));
    if (S.ev && S.ev.executions.length) head.append(execPicker());
    box.append(head, V.summary(res, d), V.notices(res) || '');
    if (!res.ok) { box.append(h('div', { class: 'card empty' }, h('p', { text: 'No physical plan to show. Try one of the samples to see what PlanReader expects.' }), h('button', { class: 'btn', type: 'button', text: 'Open samples', onclick: openSamples }))); return; }
    const tabs = [['findings', 'Findings', d.findings.filter((f) => f.severity !== 'info').length], ['tree', 'Tree'], ['graph', 'Graph'], ['text', 'Plan text']];
    const tabBar = h('div', { class: 'tabs', role: 'tablist', 'aria-label': 'Result views' }, tabs.map(([k, label, count], i) => h('button', { type: 'button', role: 'tab', id: 'tab-' + k, 'aria-controls': 'panel', 'aria-selected': String(S.view === k), tabIndex: S.view === k ? 0 : -1, onclick: () => setView(k),
      onkeydown: (e) => { const dir = e.key === 'ArrowRight' ? 1 : e.key === 'ArrowLeft' ? -1 : 0; if (dir) { e.preventDefault(); const nk = tabs[(i + dir + tabs.length) % tabs.length][0]; setView(nk); $('tab-' + nk).focus(); } } }, label, count != null ? h('span', { class: 'count', text: String(count) }) : null)));
    box.append(tabBar, toolbar(), h('div', { id: 'panel', class: 'panel', role: 'tabpanel', 'aria-labelledby': 'tab-' + S.view }));
    renderPanel();
  }
  function execPicker() {
    const sel = h('select', { id: 'execSel', 'aria-label': 'SQL execution', onchange: () => { analyze({ execId: Number(sel.value), keepTitle: false, save: false, scroll: false }); const s2 = $('execSel'); if (s2) s2.focus(); } },
      S.ev.executions.map((x) => h('option', { value: String(x.id), selected: x.id === S.execId, text: '#' + x.id + ' ' + (x.description || '(no description)').slice(0, 70) + (x.duration != null ? ' · ' + T.fmtMs(x.duration) : '') + (x.error ? ' · failed' : '') })));
    return h('div', { class: 'execpick' }, h('label', { for: 'execSel', text: S.ev.executions.length + ' SQL execution' + (S.ev.executions.length > 1 ? 's' : '') + ':' }), sel, S.ev.app.sparkVersion ? h('span', { class: 'muted small', text: 'Spark ' + S.ev.app.sparkVersion + (S.ev.app.name ? ' · ' + S.ev.app.name : '') }) : null);
  }
  function setView(k) { S.view = k; savePref(); document.querySelectorAll('.tabs [role=tab]').forEach((b) => { const on = b.id === 'tab-' + k; b.setAttribute('aria-selected', String(on)); b.tabIndex = on ? 0 : -1; }); const p = $('panel'); if (p) p.setAttribute('aria-labelledby', 'tab-' + k); const tb = document.querySelector('.vtools'); if (tb) tb.replaceWith(toolbar()); renderPanel(); }
  function toolbar() {
    const bar = h('div', { class: 'vtools' });
    if (S.view === 'tree' || S.view === 'graph') {
      const input = h('input', { type: 'search', id: 'search', placeholder: 'Search operators, tables, columns…  ( / )', 'aria-label': 'Search the plan', value: S.search, autocomplete: 'off' });
      const count = h('span', { class: 'muted small', id: 'hitCount', 'aria-live': 'polite' });
      let t; input.addEventListener('input', () => { clearTimeout(t); t = setTimeout(() => { S.search = input.value.trim(); hitIndex = -1; renderPanel(); }, 160); });
      input.addEventListener('keydown', (e) => { if (e.key === 'Enter') { e.preventDefault(); nextHit(e.shiftKey ? -1 : 1); } if (e.key === 'Escape' && input.value) { e.preventDefault(); e.stopPropagation(); input.value = ''; S.search = ''; renderPanel(); } });
      bar.append(h('div', { class: 'searchbox' }, input, count, h('button', { type: 'button', class: 'btn small', text: 'Next', 'aria-label': 'Next match', onclick: () => nextHit(1) })));
      bar.append(toggle('Metrics', S.overlay, (v) => { S.overlay = v; savePref(); renderPanel(); }, 'Show rows, sizes, spill and skew on each operator'));
      if (S.view === 'graph') bar.append(toggle('Hide plumbing', S.hidePlumbing, (v) => { S.hidePlumbing = v; savePref(); renderPanel(); }, 'Hide ColumnarToRow, InputAdapter and similar adapters'));
      if (S.view === 'tree') bar.append(h('button', { type: 'button', class: 'btn small', text: 'Expand all', onclick: () => { S.collapsed = new Set(); renderPanel(); } }), h('button', { type: 'button', class: 'btn small', text: 'Collapse exchanges', onclick: () => { S.collapsed = new Set(viewRes().nodes.filter((n) => P.isExchange(n) || n.category === 'subquery').map((n) => n.id)); renderPanel(); } }));
      if (S.res.initialPhysical) bar.append(toggle('Initial plan', S.showInitial, (v) => { S.showInitial = v; S.focus = []; renderPanel(); }, 'Show the plan before AQE re-planned it'));
    } else if (S.view === 'findings') {
      bar.append(h('span', { class: 'muted small', text: 'Findings use only what the plan shows' + (S.res.metrics ? ' plus the event-log metrics' : '') + '. Sizes are unknown unless the plan has statistics (EXPLAIN COST, a final AQE plan) or comes from an event log.' }));
    } else if (S.view === 'text') {
      bar.append(h('button', { type: 'button', class: 'btn small', text: 'Copy plan text', onclick: () => doCopy(S.text.length < 2e6 ? S.text : S.res.sections.map((s) => s.text).join('\n\n'), 'Plan text copied') }));
    }
    return bar;
  }
  function toggle(label, on, set, title) {
    const id = 'tg-' + label.replace(/\W+/g, '').toLowerCase();
    const cb = h('input', { type: 'checkbox', id, checked: on, onchange: () => set(cb.checked) });
    return h('label', { class: 'toggle', for: id, title }, cb, h('span', { text: label }));
  }
  let hitIndex = -1;
  function nextHit(dir) {
    const hits = S.view === 'graph' ? (graphApi ? graphApi.hits : []) : treeHits;
    if (!hits.length) { say(S.search ? 'No matches' : 'Type something to search'); return; }
    hitIndex = (hitIndex + dir + hits.length) % hits.length; const id = hits[hitIndex];
    if (S.view === 'graph') graphApi.focusNode(id); else { const li = document.querySelector('#panel li[data-id="' + id + '"]'); if (li) { li.scrollIntoView({ block: 'center' }); document.querySelectorAll('#panel li[role=treeitem]').forEach((x) => { x.tabIndex = -1; }); li.tabIndex = 0; li.focus({ preventScroll: true }); } }
    const c = $('hitCount'); if (c) c.textContent = (hitIndex + 1) + ' of ' + hits.length;
  }
  function renderPanel() {
    const p = $('panel'); if (!p) return; p.replaceChildren(); graphApi = null; treeHits = [];
    const res = viewRes(); const flagged = S.showInitial ? new Set() : flaggedSet();
    if (S.view === 'findings') p.append(V.findings(S.diag, { copy: doCopy, show: showNodes }));
    else if (S.view === 'tree') {
      if (S.focus.length) expandTo(S.focus);
      const t = V.tree(res, { search: S.search, overlay: S.overlay, flagged, collapsed: S.collapsed, selected: S.selected, toggle: (id, keepFocus) => { if (S.collapsed.has(id)) S.collapsed.delete(id); else S.collapsed.add(id); renderPanel(); if (keepFocus) { const li = document.querySelector('#panel li[data-id="' + id + '"]'); if (li) { document.querySelectorAll('#panel li[role=treeitem]').forEach((x) => { x.tabIndex = -1; }); li.tabIndex = 0; li.focus(); } } }, select: openNode });
      treeHits = t.hits; p.append(h('div', { class: 'treewrap card' }, t.el));
      if (S.focus.length) requestAnimationFrame(() => { const li = p.querySelector('li[data-id="' + S.focus[0] + '"]'); if (li) { li.classList.add('focus'); S.focus.forEach((id) => { const x = p.querySelector('li[data-id="' + id + '"] > .trow'); if (x) x.classList.add('focus'); }); li.scrollIntoView({ block: 'center' }); } });
    } else if (S.view === 'graph') {
      graphApi = V.graph(res, { search: S.search, overlay: S.overlay, flagged, hidePlumbing: S.hidePlumbing, focus: S.focus, selected: S.selected, select: openNode });
      p.append(graphApi.el);
      if (S.focus.some((id) => graphApi.hiddenIds.includes(id))) say('Some highlighted operators are plumbing; untick "Hide plumbing" to see them');
    } else if (S.view === 'text') {
      const secs = S.res.sections.length ? S.res.sections : [{ title: 'Input', text: S.text.slice(0, 200000) }];
      p.append(...secs.map((s) => h('details', { class: 'card sect', open: s.kind === 'physical' || secs.length === 1 }, h('summary', { text: s.title }), h('pre', { class: 'raw plan', text: s.text.length > 300000 ? s.text.slice(0, 300000) + '\n… (truncated for display)' : s.text }))));
    }
    const c = $('hitCount'); if (c) { const n = S.view === 'graph' ? (graphApi ? graphApi.hits.length : 0) : treeHits.length; c.textContent = S.search ? n + ' match' + (n === 1 ? '' : 'es') : ''; }
  }
  function expandTo(ids) { const byId = {}; viewRes().nodes.forEach((n) => { byId[n.id] = n; }); ids.forEach((id) => { let n = byId[id]; while (n && n.parentId) { S.collapsed.delete(n.parentId); n = byId[n.parentId]; } }); }
  function showNodes(ids, view) { S.showInitial = false; S.focus = ids.slice(); S.search = ''; setView(view); }
  function openNode(id) {
    const res = viewRes(); const n = res.nodes.find((x) => x.id === id) || S.res.nodes.find((x) => x.id === id); if (!n) return;
    S.selected = id;
    const m = openModal({ title: n.name + (P.shortLabel(n) ? ' · ' + T.stripIds(P.shortLabel(n)) : ''), size: 'wide', cancelLabel: 'Close',
      body: V.detailBody(n, res, S.diag, { gotoFinding: (fid) => { m.closeNow().then(() => { setView('findings'); requestAnimationFrame(() => { const el = $('f-' + fid); if (el) { el.scrollIntoView({ block: 'start' }); el.classList.add('pulse'); el.tabIndex = -1; el.focus({ preventScroll: true }); } }); }); } }),
      actions: [{ label: 'Copy line', onClick: () => { doCopy(n.text, 'Plan line copied'); return false; } }],
      onClose: () => { S.selected = null; } });
  }
  function welcome() {
    const w = h('div', { class: 'welcome card', id: 'welcome' }, h('h1', { text: 'See what your Spark job really does' }), h('p', { html: 'Paste the output of <code>df.explain()</code>, <code>EXPLAIN FORMATTED</code> or a Spark event log. PlanReader draws the plan, marks stages and codegen blocks, and points out shuffles, joins that could be broadcast, skew, Python UDFs, missing partition pruning and more, with fixes you can copy.' }),
      h('div', { class: 'welcome-actions' }, h('button', { class: 'btn primary', type: 'button', text: 'Open a sample plan', 'data-sample': 'smj-aqe-off', onclick: () => loadSample('smj-aqe-off') }), ' ', h('button', { class: 'btn', type: 'button', text: 'Open a sample event log', 'data-sample': 'eventlog', onclick: () => loadSample('eventlog') }), ' ', h('button', { class: 'btn', type: 'button', text: 'Compare a before/after', 'data-pair': 'broadcast', onclick: () => loadPair('broadcast') })));
    return w;
  }

  // ---------- compare ----------
  function compare(opts) {
    opts = opts || {};
    const a = S.cmpFiles.a ? S.cmpFiles.a.text : $('cmpA').value, b = S.cmpFiles.b ? S.cmpFiles.b.text : $('cmpB').value;
    if (!a.trim() || !b.trim()) { say('Paste both plans first'); (!a.trim() ? $('cmpA') : $('cmpB')).focus(); return; }
    const ra = runAnalysis(a), rb = runAnalysis(b);
    if (!ra.res.ok || !rb.res.ok) { S.cmp = { error: (!ra.res.ok ? 'The "before" plan' : 'The "after" plan') + ' has no physical plan PlanReader can read.', ra, rb }; render(); return; }
    const diff = X.diffPlans(ra.res, rb.res);
    S.cmp = { ra, rb, diff, textA: a, textB: b, title: opts.title || ('Compare: ' + autoTitle(ra.res)) };
    render(); if (opts.save !== false) saveHistory('compare');
    if (window.innerWidth < 900) $('results').scrollIntoView({ behavior: 'smooth', block: 'start' });
  }
  function renderCompare(box) {
    if (!S.cmp) { box.append(h('div', { class: 'card welcome' }, h('h2', { text: 'Did your fix change the plan?' }), h('p', { text: 'Paste the plan before and after a change (a broadcast hint, a native function instead of a UDF, a partition filter…). PlanReader shows which operators, joins, scans and shuffles changed, which findings were resolved, and a line-by-line diff of the plan tree.' }), h('button', { class: 'btn primary', type: 'button', text: 'Load a sample pair', onclick: () => loadPair('broadcast') }))); return; }
    if (S.cmp.error) { box.append(h('div', { class: 'notices' }, h('div', { class: 'notice error', text: S.cmp.error }))); return; }
    const { diff } = S.cmp;
    const sc = (v) => h('span', { class: 'score ' + (v >= 80 ? 'good' : v >= 55 ? 'ok' : 'bad'), text: String(v) });
    box.append(h('div', { class: 'reshead' }, h('h2', { class: 'restitle', text: S.cmp.title })));
    box.append(h('div', { class: 'card cmpsum' }, h('div', { class: 'scores' }, h('div', null, h('div', { class: 'muted small', text: 'Before' }), sc(diff.score.before)), h('div', { class: 'arrow', 'aria-hidden': 'true', text: '→' }), h('div', null, h('div', { class: 'muted small', text: 'After' }), sc(diff.score.after))),
      h('ul', { class: 'cmplist' }, diff.summary.map((s) => h('li', { text: s })))));
    const sec = (title, kids) => h('section', { class: 'card' }, h('h3', { text: title }), kids);
    if (diff.resolved.length || diff.added.length) box.append(sec('Findings', h('ul', { class: 'cmpf' }, diff.resolved.map((f) => h('li', { class: 'good' }, h('span', { class: 'sev sev-' + f.severity, text: f.severity }), ' Resolved: ' + f.title)), diff.added.map((f) => h('li', { class: 'bad' }, h('span', { class: 'sev sev-' + f.severity, text: f.severity }), ' New: ' + f.title)))));
    if (diff.ops.length) box.append(sec('Operators', h('div', { class: 'tablewrap' }, h('table', { class: 'mtable' }, h('thead', null, h('tr', null, h('th', { scope: 'col', text: 'Operator' }), h('th', { scope: 'col', text: 'Before' }), h('th', { scope: 'col', text: 'After' }), h('th', { scope: 'col', text: 'Change' }))),
      h('tbody', null, diff.ops.map((o) => h('tr', null, h('td', null, h('span', { 'data-gloss': o.name, text: o.name })), h('td', { text: String(o.before) }), h('td', { text: String(o.after) }), h('td', { class: o.delta > 0 ? 'plus' : 'minus', text: (o.delta > 0 ? '+' : '') + o.delta }))))))));
    if (diff.joinChanges.length) box.append(sec('Joins', h('ul', null, diff.joinChanges.map((c) => h('li', { text: c.before && c.after ? c.before.tables + ': ' + c.before.strategy + (c.before.type ? ' (' + c.before.type + ')' : '') + ' → ' + c.after.strategy + (c.after.type ? ' (' + c.after.type + ')' : '') : c.before ? 'Removed: ' + c.before.strategy + ' (' + c.before.tables + ')' : 'Added: ' + c.after.strategy + ' (' + c.after.tables + ')' })))));
    if (diff.scanChanges.length) box.append(sec('Scans', h('ul', null, diff.scanChanges.map((s) => h('li', null, h('strong', { text: s.table }), ': ' + s.notes.join('; '))))));
    const lines = diff.lines; const changed = lines.filter((l) => l.op !== ' ').length;
    box.append(sec('Plan tree diff', [h('p', { class: 'muted small', text: changed ? changed + ' changed lines. Expression ids (#12) and plan ids are ignored.' : 'The plan trees are the same once expression ids are ignored.' }),
      h('pre', { class: 'raw diff' }, lines.map((l) => h('span', { class: l.op === '+' ? 'add' : l.op === '-' ? 'del' : 'same', text: l.op + ' ' + (l.b || l.a).text + '\n' })))]));
    box.append(h('div', { class: 'actions' }, h('button', { class: 'btn', type: 'button', text: 'Open "before" in Analyze', onclick: () => openInAnalyze(S.cmp.textA, 'Before') }), h('button', { class: 'btn', type: 'button', text: 'Open "after" in Analyze', onclick: () => openInAnalyze(S.cmp.textB, 'After') })));
  }
  function openInAnalyze(text, title) { setMode('analyze'); setText(text, null); analyze({ title }); }
  function setMode(m) {
    S.mode = m; $('modeAnalyze').setAttribute('aria-pressed', String(m === 'analyze')); $('modeCompare').setAttribute('aria-pressed', String(m === 'compare'));
    $('analyzePane').hidden = m !== 'analyze'; $('comparePane').hidden = m !== 'compare'; render();
  }

  // ---------- input ----------
  function setText(text, file) {
    S.file = file || null; const ta = $('planText'); const chip = $('fileChip');
    if (file) { ta.value = ''; ta.hidden = true; chip.hidden = false; chip.replaceChildren(h('span', { class: 'fname', text: file.name }), h('span', { class: 'muted small', text: T.fmtBytes(file.text.length) + (E.looksLikeEventLog(file.text) ? ' · event log' : '') }), h('button', { type: 'button', class: 'btn small', text: 'Edit as text', onclick: () => { if (file.text.length > 3e6) { say('Too large to edit comfortably here'); return; } const t = file.text; S.file = null; chip.hidden = true; ta.hidden = false; ta.value = t; ta.focus(); } }), h('button', { type: 'button', class: 'btn small ghost', text: 'Remove', 'aria-label': 'Remove file ' + file.name, onclick: clearInput })); }
    else { chip.hidden = true; ta.hidden = false; ta.value = text; }
  }
  function clearInput() {
    const prev = { text: $('planText').value, file: S.file, res: S.res, diag: S.diag, ev: S.ev, title: S.title, execId: S.execId };
    if (!prev.text && !prev.file && !prev.res) return;
    setText('', null); S.res = null; S.diag = null; S.ev = null; render(); $('planText').focus();
    say('Cleared', { action: 'Undo', onAction: () => { setText(prev.text, prev.file); Object.assign(S, { res: prev.res, diag: prev.diag, ev: prev.ev, title: prev.title, execId: prev.execId }); render(); } });
  }
  function readFile(f) { return new Promise((res, rej) => { const r = new FileReader(); r.onload = () => res(String(r.result)); r.onerror = () => rej(r.error); r.readAsText(f); }); }
  async function openFile(f, target) {
    if (!f) return;
    if (f.size > 300 * 1024 * 1024) { say('That file is over 300 MB. Try the event log of a single application.'); return; }
    if (/\.(zst|lz4|snappy|gz|zip)$/i.test(f.name)) { say('Compressed event logs are not supported. Decompress it first (for example zstd -d).'); return; }
    let text; try { text = await readFile(f); } catch (e) { say('Could not read ' + f.name); return; }
    if (target) { const big = text.length > 200000; S.cmpFiles[target] = big ? { name: f.name, text } : null; const ta = $(target === 'a' ? 'cmpA' : 'cmpB'); ta.value = big ? '' : text; ta.placeholder = big ? f.name + ' loaded (' + T.fmtBytes(text.length) + '). Type here to replace it.' : ta.placeholder; return; }
    const big = text.length > 200000 || E.looksLikeEventLog(text);
    setText(text, big ? { name: f.name, text } : null); S.file = big ? { name: f.name, text } : null;
    S.title = f.name; analyze({ title: f.name });
  }
  ['cmpA', 'cmpB'].forEach((id) => $(id).addEventListener('input', () => { S.cmpFiles[id === 'cmpA' ? 'a' : 'b'] = null; }));

  // ---------- samples ----------
  async function fetchSample(id) {
    const r = await fetch(SP.samples.urlFor(id)); if (!r.ok) throw new Error('HTTP ' + r.status); return r.text();
  }
  async function loadSample(id) {
    const s = SP.samples.list.find((x) => x.id === id); if (!s) return;
    let text; try { text = await fetchSample(id); } catch (e) { say('Could not load the sample (offline and not cached yet?)'); return; }
    const prev = { text: $('planText').value, file: S.file, res: S.res, diag: S.diag, ev: S.ev, title: S.title, execId: S.execId, mode: S.mode };
    if (S.mode !== 'analyze') setMode('analyze');
    const isEv = E.looksLikeEventLog(text);
    setText(text, isEv ? { name: s.file || (id + '.json'), text } : null);
    analyze({ title: s.title });
    if (prev.text || prev.file) say('Loaded sample: ' + s.title, { action: 'Undo', onAction: () => { setText(prev.text, prev.file); Object.assign(S, { res: prev.res, diag: prev.diag, ev: prev.ev, title: prev.title, execId: prev.execId }); setMode(prev.mode); } });
  }
  async function loadPair(key) {
    const p = SP.samples.fixPairs.find((x) => x.id === key); if (!p) return;
    let a, b; try { [a, b] = await Promise.all([fetchSample(p.before), fetchSample(p.after)]); } catch (e) { say('Could not load the samples (offline and not cached yet?)'); return; }
    const prev = { a: $('cmpA').value, b: $('cmpB').value, files: Object.assign({}, S.cmpFiles), cmp: S.cmp };
    setMode('compare'); S.cmpFiles = { a: null, b: null }; $('cmpA').value = a; $('cmpB').value = b; $('fixPreset').value = key;
    compare({ title: p.title });
    if (prev.a || prev.b) say('Loaded pair: ' + p.title, { action: 'Undo', onAction: () => { $('cmpA').value = prev.a; $('cmpB').value = prev.b; S.cmpFiles = prev.files; S.cmp = prev.cmp; $('fixPreset').value = ''; render(); } });
  }
  function openSamples() {
    const groups = {}; SP.samples.list.forEach((s) => { (groups[s.group] = groups[s.group] || []).push(s); });
    let m;
    const body = h('div', { class: 'samples' },
      h('p', { class: 'muted small', text: 'Generated with PySpark 4.0.1 and 3.5.7 on local data (except where noted). Loading one replaces the current input; you can undo it.' }),
      Object.entries(groups).map(([g, items]) => h('section', null, h('h3', { text: g }), h('ul', { class: 'samplelist' }, items.map((s) => h('li', null, h('button', { type: 'button', class: 'sample', onclick: () => { m.closeNow().then(() => loadSample(s.id)); } }, h('strong', { text: s.title }), h('span', { class: 'muted small', text: s.note || '' }))))))),
      h('section', null, h('h3', { text: 'Before / after a fix' }), h('ul', { class: 'samplelist' }, SP.samples.fixPairs.map((p) => h('li', null, h('button', { type: 'button', class: 'sample', onclick: () => { m.closeNow().then(() => loadPair(p.id)); } }, h('strong', { text: p.title }), h('code', { class: 'small', text: p.code || '' })))))));
    m = openModal({ title: 'Sample plans', body, size: 'wide', cancelLabel: 'Close' });
  }

  // ---------- history ----------
  function openHistory() {
    const list = h('ul', { class: 'histlist' }); const foot = h('div');
    let m;
    const draw = () => {
      const all = store.history.list();
      list.replaceChildren(...(all.length ? all.map((e) => h('li', { class: 'hist' },
        h('button', { type: 'button', class: 'histopen', disabled: e.kind === 'analyze' ? e.text == null : (e.a == null || e.b == null), title: (e.kind === 'analyze' ? e.text == null : (e.a == null || e.b == null)) ? 'Too large to keep in history; open the file again' : 'Open', onclick: () => { m.closeNow().then(() => openEntry(e)); } },
          h('strong', { text: e.title || 'Untitled' }), h('span', { class: 'muted small', text: [e.kind === 'compare' ? 'comparison' : (e.format || 'plan'), e.version ? (/^Spark/.test(e.version) ? e.version : 'Spark ' + e.version) : '', 'score ' + e.score, new Date(e.savedAt).toLocaleString()].filter(Boolean).join(' · ') })),
        h('button', { type: 'button', class: 'btn small', text: 'Rename', 'aria-label': 'Rename ' + (e.title || 'entry'), onclick: () => renameEntry(e, draw) }),
        h('button', { type: 'button', class: 'btn small danger', text: 'Delete', 'aria-label': 'Delete ' + (e.title || 'entry'), onclick: () => { const r = store.history.remove(e.id); draw(); say('Deleted "' + (e.title || 'entry') + '"', { action: 'Undo', onAction: () => { store.history.restore(r.item, r.index); draw(); } }); } })))
        : [h('li', { class: 'empty muted', text: 'Nothing yet. Plans you analyze are kept here, in this browser only.' })]));
      foot.replaceChildren(all.length ? h('button', { type: 'button', class: 'btn small danger', text: 'Clear all', onclick: () => { const items = store.history.clear(); draw(); say('Cleared ' + items.length + ' entries', { action: 'Undo', onAction: () => { store.history.restoreAll(items); draw(); } }); } }) : '');
    };
    draw();
    m = openModal({ title: 'History', body: h('div', null, h('p', { class: 'muted small', text: 'Stored in this browser (localStorage), newest first, up to 60 entries.' }), list), footLeft: foot, cancelLabel: 'Close' });
  }
  function openEntry(e) {
    if (e.kind === 'compare') { setMode('compare'); S.cmpFiles = { a: null, b: null }; $('cmpA').value = e.a; $('cmpB').value = e.b; compare({ title: e.title, save: false }); return; }
    setMode('analyze'); const isFile = e.fileName && (e.text.length > 200000 || E.looksLikeEventLog(e.text));
    setText(e.text, isFile ? { name: e.fileName, text: e.text } : null); analyze({ title: e.title, execId: e.execId, save: false });
  }
  function renameEntry(e, after) {
    const orig = e.title || '';
    const input = h('input', { type: 'text', id: 'renameInput', value: orig, maxlength: 120, required: true, 'aria-describedby': 'renameErr', autocomplete: 'off' });
    const err = h('p', { id: 'renameErr', class: 'err', 'aria-live': 'polite' });
    let m;
    const save = () => { const v = input.value.trim(); if (!v) { err.textContent = 'Enter a name, or press Cancel to keep "' + orig + '".'; input.setAttribute('aria-invalid', 'true'); input.focus(); return false; } store.history.update(e.id, { title: v }); if (S.historyId === e.id) { S.title = v; const t = document.querySelector('.restitle'); if (t) t.textContent = v; } after(); say('Renamed'); return true; };
    const form = h('form', { onsubmit: (ev) => { ev.preventDefault(); if (save()) m.closeNow(); } }, h('label', { class: 'lab', for: 'renameInput', text: 'Name' }), input, err);
    m = openModal({ title: 'Rename', body: form, isDirty: () => input.value !== orig, confirmText: 'Discard the new name?', actions: [{ label: 'Save', kind: 'primary', onClick: () => save() }] });
  }

  // ---------- share / export ----------
  async function openShare() {
    let payload;
    if (S.mode === 'compare') { if (!S.cmp || S.cmp.error) { say('Compare two plans first'); return; } payload = { v: 1, mode: 'compare', a: S.cmp.textA, b: S.cmp.textB, title: S.cmp.title }; }
    else { if (!S.res) { say('Analyze a plan first'); return; } payload = { v: 1, mode: 'analyze', text: S.text, title: S.title, exec: S.execId }; }
    const enc = await SP.share.encode(payload);
    const url = location.href.split('#')[0] + '#p=' + enc;
    const tooBig = enc.length > SHARE_LIMIT;
    const field = h('input', { type: 'text', id: 'shareUrl', readOnly: true, value: tooBig ? '' : url, 'aria-label': 'Share link', onfocus: (e) => e.target.select() });
    const body = h('div', null,
      tooBig ? h('div', { class: 'notice warn', text: 'This input is too large for a link (' + T.fmtBytes(enc.length) + ' after compression; the limit is about ' + T.fmtBytes(SHARE_LIMIT) + '). Export a report instead, or share a single EXPLAIN rather than the whole event log.' }) : null,
      tooBig ? null : field,
      h('p', { class: 'muted small', text: 'The plan is compressed into the part of the link after #. That part is never sent to a server, but anyone you give the link to can read the plan, including table and column names.' }));
    openModal({ title: 'Share link', body, cancelLabel: 'Close', initialFocus: tooBig ? null : '#shareUrl', actions: tooBig ? [{ label: 'Export instead', onClick: () => { setTimeout(openExport, 0); } }] : [{ label: 'Copy link', kind: 'primary', onClick: () => { doCopy(url, 'Link copied'); return false; } }] });
  }
  function slug(s) { return (s || 'plan').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '').slice(0, 50) || 'plan'; }
  function openExport() {
    let md, json, title;
    if (S.mode === 'compare') { if (!S.cmp || S.cmp.error) { say('Compare two plans first'); return; } title = S.cmp.title; md = R.diffMarkdown(S.cmp.diff); json = JSON.stringify({ tool: 'Spark PlanReader', title, score: S.cmp.diff.score, summary: S.cmp.diff.summary, ops: S.cmp.diff.ops, joinChanges: S.cmp.diff.joinChanges, scanChanges: S.cmp.diff.scanChanges, resolved: S.cmp.diff.resolved.map((f) => f.title), added: S.cmp.diff.added.map((f) => f.title) }, null, 2); }
    else { if (!S.res) { say('Analyze a plan first'); return; } title = S.title || 'Plan'; md = R.markdown(S.res, S.diag, { title }); json = R.json(S.res, S.diag); }
    const name = slug(title);
    const pre = h('pre', { class: 'raw preview', tabindex: '0', 'aria-label': 'Markdown preview', text: md });
    openModal({ title: 'Export report', size: 'wide', cancelLabel: 'Close', body: h('div', null, h('p', { class: 'muted small', text: 'Reports include findings, fixes and the operator tree. They are generated here; nothing is uploaded.' }), pre),
      footLeft: h('button', { type: 'button', class: 'btn small', text: 'Copy Markdown', onclick: () => doCopy(md, 'Markdown copied') }),
      actions: [{ label: 'JSON', onClick: () => { download(name + '.json', json, 'application/json'); return false; } }, { label: 'HTML', onClick: () => { download(name + '.html', R.html(md, title), 'text/html'); return false; } }, { label: 'Markdown', kind: 'primary', onClick: () => { download(name + '.md', md, 'text/markdown'); return false; } }] });
  }

  // ---------- glossary / help ----------
  function openGlossary(q) {
    const input = h('input', { type: 'search', id: 'glossSearch', placeholder: 'Filter operators…', 'aria-label': 'Filter glossary', value: q || '', autocomplete: 'off' });
    const list = h('dl', { class: 'glosslist' });
    const draw = () => { const v = input.value.trim().toLowerCase(); const items = Object.entries(SP.glossary.entries).map(([name, text]) => ({ name, text })).sort((x, y) => x.name.localeCompare(y.name)).filter((e) => !v || (e.name + ' ' + e.text).toLowerCase().includes(v)); list.replaceChildren(...(items.length ? items.flatMap((e) => [h('dt', { text: e.name }), h('dd', { text: e.text })]) : [h('dd', { class: 'muted', text: 'No operator matches.' })])); };
    input.addEventListener('input', draw); draw();
    openModal({ title: 'Operator glossary', size: 'wide', cancelLabel: 'Close', body: h('div', null, input, list) });
  }
  function openHelp() {
    const keys = [['Ctrl/⌘ + Enter', 'Analyze or compare'], ['/', 'Search the tree or graph'], ['Enter / Shift+Enter in search', 'Next / previous match'], ['f  t  g  p', 'Findings, Tree, Graph, Plan text'], ['+  −  0, arrow keys', 'Zoom, fit and pan the graph'], ['Arrow keys in the tree', 'Move, expand, collapse; Enter opens details'], ['Esc or Back', 'Close the top dialog'], ['?', 'This help']];
    openModal({ title: 'Help', cancelLabel: 'Close', body: h('div', { class: 'help' },
      h('h3', { text: 'Getting a plan' }), h('pre', { class: 'raw', text: '# PySpark\ndf.explain(mode="formatted")   # or "extended", "cost", "codegen"\n\n-- SQL\nEXPLAIN FORMATTED SELECT ...\n\n# Event logs: spark.eventLog.enabled=true, then open the file from spark.eventLog.dir\n# (uncompressed; set spark.eventLog.compress=false or decompress first)' }),
      h('h3', { text: 'Keyboard' }), h('table', { class: 'mtable' }, h('tbody', null, keys.map(([k, v]) => h('tr', null, h('th', { scope: 'row' }, h('kbd', { text: k })), h('td', { text: v }))))),
      h('h3', { text: 'Privacy' }), h('p', { text: 'Everything runs in this page. Plans are never uploaded; history is kept in this browser only. The app works offline after the first visit.' }),
      h('h3', { text: 'Limits' }), h('p', { text: 'Without statistics or metrics, data sizes are unknown, so findings about sizes are hedged. Event-log metric mapping is best effort. Plans from other engines are not supported.' }),
      h('p', { class: 'muted small' }, 'Open source (MIT): ', h('a', { href: 'https://github.com/shaikbasha1437/spark-planreader', target: '_blank', rel: 'noopener', text: 'github.com/shaikbasha1437/spark-planreader' }))) });
  }
  function applyTheme(t) { document.documentElement.dataset.theme = t; const b = $('btnTheme'); b.setAttribute('aria-label', t === 'dark' ? 'Switch to light theme' : 'Switch to dark theme'); const mc = document.querySelector('meta[name=theme-color]'); if (mc) mc.content = t === 'dark' ? '#16181d' : '#e25a1c'; }

  // ---------- wiring ----------
  $('btnAnalyze').addEventListener('click', () => analyze());
  $('btnClear').addEventListener('click', clearInput);
  $('btnTrySample').addEventListener('click', openSamples);
  $('btnSamples').addEventListener('click', openSamples);
  $('btnHistory').addEventListener('click', openHistory);
  $('btnShare').addEventListener('click', openShare);
  $('btnExport').addEventListener('click', openExport);
  $('btnGlossary').addEventListener('click', () => openGlossary());
  $('btnHelp').addEventListener('click', openHelp);
  $('btnTheme').addEventListener('click', () => { const t = document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark'; try { localStorage.setItem('spark-planreader:theme', t); } catch (e) { /* ignore */ } applyTheme(t); });
  $('modeAnalyze').addEventListener('click', () => setMode('analyze'));
  $('modeCompare').addEventListener('click', () => setMode('compare'));
  $('btnCompare').addEventListener('click', () => compare());
  $('btnSwap').addEventListener('click', () => { const a = $('cmpA').value; $('cmpA').value = $('cmpB').value; $('cmpB').value = a; S.cmpFiles = { a: S.cmpFiles.b, b: S.cmpFiles.a }; if (S.cmp && !S.cmp.error) compare({ save: false }); });
  $('btnClearCmp').addEventListener('click', () => { const prev = { a: $('cmpA').value, b: $('cmpB').value, files: S.cmpFiles, cmp: S.cmp }; $('cmpA').value = ''; $('cmpB').value = ''; S.cmpFiles = { a: null, b: null }; S.cmp = null; $('fixPreset').value = ''; render(); say('Cleared', { action: 'Undo', onAction: () => { $('cmpA').value = prev.a; $('cmpB').value = prev.b; S.cmpFiles = prev.files; S.cmp = prev.cmp; render(); } }); });
  SP.samples.fixPairs.forEach((p) => $('fixPreset').append(h('option', { value: p.id, text: p.title })));
  $('fixPreset').addEventListener('change', (e) => { if (e.target.value) loadPair(e.target.value); });
  $('fileInput').addEventListener('change', (e) => { openFile(e.target.files[0]); e.target.value = ''; });
  $('fileA').addEventListener('change', (e) => { openFile(e.target.files[0], 'a'); e.target.value = ''; });
  $('fileB').addEventListener('change', (e) => { openFile(e.target.files[0], 'b'); e.target.value = ''; });
  document.querySelectorAll('label[role=button][for]').forEach((l) => l.addEventListener('keydown', (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); $(l.getAttribute('for')).click(); } }));
  [['planText', null], ['cmpA', 'a'], ['cmpB', 'b']].forEach(([id, target]) => {
    const el = $(id); const zone = id === 'planText' ? el.parentElement : el;
    zone.addEventListener('dragover', (e) => { e.preventDefault(); zone.classList.add('drop'); });
    zone.addEventListener('dragleave', () => zone.classList.remove('drop'));
    zone.addEventListener('drop', (e) => { e.preventDefault(); zone.classList.remove('drop'); const f = e.dataTransfer.files[0]; if (f) openFile(f, target); });
  });
  document.addEventListener('keydown', (e) => {
    const typing = e.target.closest && e.target.closest('input, textarea, select, [contenteditable]');
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') { if (stack.size) return; e.preventDefault(); if (S.mode === 'compare') compare(); else analyze(); return; }
    if (typing || stack.size || e.ctrlKey || e.metaKey || e.altKey) return;
    if (e.key === '?') { e.preventDefault(); openHelp(); return; }
    if (e.key === '/' && S.res && S.mode === 'analyze') { e.preventDefault(); if (S.view !== 'tree' && S.view !== 'graph') setView('tree'); const s = $('search'); if (s) s.focus(); return; }
    const map = { f: 'findings', t: 'tree', g: 'graph', p: 'text' };
    if (map[e.key] && S.res && S.res.ok && S.mode === 'analyze' && !e.target.closest('svg')) { e.preventDefault(); setView(map[e.key]); }
  });

  // ---------- startup ----------
  async function fromHash() {
    const m = /^#p=(.+)$/.exec(location.hash); if (!m) return false;
    let p; try { p = await SP.share.decode(m[1]); } catch (e) { say('That share link is damaged or incomplete'); return false; }
    if (!p || p.v !== 1) { say('That share link is from a newer version'); return false; }
    if (p.mode === 'compare') { setMode('compare'); $('cmpA').value = p.a || ''; $('cmpB').value = p.b || ''; compare({ title: p.title, save: false }); }
    else { const isEv = E.looksLikeEventLog(p.text || ''); setText(p.text || '', isEv ? { name: 'shared event log', text: p.text } : null); analyze({ title: p.title, execId: p.exec, save: false, scroll: false }); }
    return true;
  }
  applyTheme(document.documentElement.dataset.theme);
  render();
  fromHash();
  window.addEventListener('hashchange', () => { if (/^#p=/.test(location.hash)) fromHash(); });
  if ('serviceWorker' in navigator && location.protocol !== 'file:') window.addEventListener('load', () => navigator.serviceWorker.register('sw.js').catch(() => { /* offline support is optional */ }));
  root.SP.app = { S, analyze, compare, loadSample, loadPair };
})(self);
