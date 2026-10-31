/* Spark PlanReader plan diff: what changed between two plans (before / after a fix). */
(function (root, factory) {
  const m = factory(root.SP ? root.SP.parser : require('./parser.js'), root.SP ? root.SP.text : require('./textutil.js'), root.SP ? root.SP.diagnose : require('./diagnose.js'));
  if (typeof module === 'object' && module.exports) module.exports = m;
  else (root.SP = root.SP || {}).diff = m;
})(typeof self !== 'undefined' ? self : this, function (P, T, D) {
  'use strict';
  const opKey = (n) => n.name;
  function counts(res) { const c = {}; (res.nodes || []).forEach((n) => { if (n.category === 'marker') return; const k = opKey(n); c[k] = (c[k] || 0) + 1; }); return c; }
  function treeLines(res) {
    const out = []; if (!res.physical) return out;
    P.walk(res.physical, (n, d) => { if (n.category === 'marker') return; const lab = P.shortLabel(n); out.push({ text: '  '.repeat(d) + (n.codegen ? '*' : '') + n.name + (lab ? '  ' + T.stripIds(lab) : ''), id: n.id }); });
    (res.subqueries || []).forEach((s) => P.walk(s, (n, d) => out.push({ text: '  '.repeat(d + 1) + n.name + ' ' + T.stripIds(P.shortLabel(n)), id: n.id })));
    return out;
  }
  // Longest common subsequence line diff; plans are small enough (hundreds of lines) for O(n*m).
  function lineDiff(a, b) {
    const n = a.length, m = b.length; const key = (x) => x.text.trim();
    if (n * m > 4e6) return a.map((x) => ({ op: '-', a: x })).concat(b.map((x) => ({ op: '+', b: x })));
    const L = Array.from({ length: n + 1 }, () => new Uint16Array(m + 1));
    for (let i = n - 1; i >= 0; i--) for (let j = m - 1; j >= 0; j--) L[i][j] = key(a[i]) === key(b[j]) ? L[i + 1][j + 1] + 1 : Math.max(L[i + 1][j], L[i][j + 1]);
    const out = []; let i = 0, j = 0;
    while (i < n && j < m) { if (key(a[i]) === key(b[j])) { out.push({ op: ' ', a: a[i], b: b[j] }); i++; j++; } else if (L[i + 1][j] >= L[i][j + 1]) { out.push({ op: '-', a: a[i] }); i++; } else { out.push({ op: '+', b: b[j] }); j++; } }
    while (i < n) out.push({ op: '-', a: a[i++] }); while (j < m) out.push({ op: '+', b: b[j++] });
    return out;
  }
  function joinsOf(res) {
    return (res.nodes || []).filter((n) => n.category === 'join').map((n) => {
      const sides = n.children.slice(-2).map((c) => D.tableOf(c) || '?');
      return { id: n.id, strategy: n.name, type: n.attrs.joinType || '', tables: sides.slice().sort().join(' × '), keys: (n.attrs.leftKeys || []).map(T.attrName).join(', ') };
    });
  }
  function scansOf(res) {
    return (res.nodes || []).filter((n) => n.category === 'scan' && /^(FileScan|Scan|BatchScan|PhotonScan)/.test(n.name)).map((n) => ({ id: n.id, table: D.tableOf(n) || n.name, cols: (n.attrs.readCols || []).length, partitionFilters: (n.attrs.partitionFilters || []).map(T.stripIds), pushedFilters: (n.attrs.pushedFilters || []).filter((p) => !/^IsNotNull/.test(p)), format: n.attrs.format || '' }));
  }
  const fKey = (f) => (f.key || f.rule + '|' + T.stripIds(f.title)).replace(/\d+(\.\d+)?\s*(KiB|MiB|GiB|B|×)/g, '#');
  function diffPlans(a, b) {
    const da = D.diagnose(a), db = D.diagnose(b);
    const ca = counts(a), cb = counts(b);
    const ops = []; new Set([...Object.keys(ca), ...Object.keys(cb)]).forEach((k) => { const x = ca[k] || 0, y = cb[k] || 0; if (x !== y) ops.push({ name: k, before: x, after: y, delta: y - x }); });
    ops.sort((p, q) => Math.abs(q.delta) - Math.abs(p.delta) || p.name.localeCompare(q.name));
    // Join strategy changes: match joins by the tables they join, then by position.
    const ja = joinsOf(a), jb = joinsOf(b); const used = new Set(); const joinChanges = [];
    ja.forEach((x, i) => {
      let y = jb.find((z, k) => !used.has(k) && z.tables === x.tables); if (!y) y = jb.find((z, k) => !used.has(k) && k === i);
      if (!y) { joinChanges.push({ before: x, after: null }); return; } used.add(jb.indexOf(y));
      if (y.strategy !== x.strategy || y.type !== x.type) joinChanges.push({ before: x, after: y });
    });
    jb.forEach((y, k) => { if (!used.has(k)) joinChanges.push({ before: null, after: y }); });
    // Scan changes by table
    const sa = scansOf(a), sb = scansOf(b); const scanChanges = [];
    const tables = [...new Set(sa.map((s) => s.table).concat(sb.map((s) => s.table)))];
    tables.forEach((t) => {
      const x = sa.filter((s) => s.table === t), y = sb.filter((s) => s.table === t);
      const sum = (arr, f) => arr.map(f); const notes = [];
      if (x.length !== y.length) notes.push('scans ' + x.length + ' → ' + y.length);
      const pfA = sum(x, (s) => s.partitionFilters.length).reduce((p, q) => p + q, 0), pfB = sum(y, (s) => s.partitionFilters.length).reduce((p, q) => p + q, 0);
      if (!pfA && pfB) notes.push('partition pruning added: ' + y.flatMap((s) => s.partitionFilters).join(', ')); else if (pfA && !pfB) notes.push('partition pruning lost');
      const puA = x.flatMap((s) => s.pushedFilters), puB = y.flatMap((s) => s.pushedFilters);
      if (puA.join() !== puB.join()) notes.push('pushed filters: [' + puA.join(', ') + '] → [' + puB.join(', ') + ']');
      const cA = x.map((s) => s.cols).join('+'), cB = y.map((s) => s.cols).join('+'); if (cA !== cB && x.length === y.length) notes.push('columns read ' + cA + ' → ' + cB);
      const fA = [...new Set(x.map((s) => s.format))].join(), fB = [...new Set(y.map((s) => s.format))].join(); if (x.length && y.length && fA !== fB) notes.push('format ' + fA + ' → ' + fB);
      if (notes.length) scanChanges.push({ table: t, notes });
    });
    const ka = new Map(da.findings.map((f) => [fKey(f), f])), kb = new Map(db.findings.map((f) => [fKey(f), f]));
    const resolved = da.findings.filter((f) => !kb.has(fKey(f)) && f.severity !== 'info');
    const added = db.findings.filter((f) => !ka.has(fKey(f)) && f.severity !== 'info');
    const lines = lineDiff(treeLines(a), treeLines(b));
    const sh = { before: da.summary.shuffles, after: db.summary.shuffles };
    const summary = [];
    if (sh.before !== sh.after) summary.push('Shuffles ' + sh.before + ' → ' + sh.after + (sh.after < sh.before ? ' (' + (sh.before - sh.after) + ' fewer)' : ''));
    joinChanges.forEach((c) => { if (c.before && c.after) summary.push((c.before.tables) + ': ' + c.before.strategy + ' → ' + c.after.strategy); else if (c.before) summary.push('Join removed: ' + c.before.strategy + ' (' + c.before.tables + ')'); else summary.push('Join added: ' + c.after.strategy + ' (' + c.after.tables + ')'); });
    scanChanges.forEach((s) => summary.push(s.table + ': ' + s.notes.join('; ')));
    ops.filter((o) => /Sort$|EvalPython|Expand|Window|Coalesce|Exchange|NestedLoop|Cartesian/.test(o.name) && !summary.some((x) => x.includes(o.name))).forEach((o) => summary.push(o.name + ' ' + o.before + ' → ' + o.after));
    if (resolved.length) summary.push(resolved.length + ' finding' + (resolved.length > 1 ? 's' : '') + ' resolved');
    if (added.length) summary.push(added.length + ' new finding' + (added.length > 1 ? 's' : ''));
    if (!summary.length) summary.push(lines.every((l) => l.op === ' ') ? 'The two plans have the same operators.' : 'Small changes only (see the tree diff).');
    return { ops, joinChanges, scanChanges, shuffles: sh, score: { before: da.summary.score, after: db.summary.score }, resolved, added, lines, summary, before: da, after: db };
  }
  return { diffPlans, lineDiff, treeLines };
});
