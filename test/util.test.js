'use strict';
const test = require('node:test'); const assert = require('node:assert/strict');
const fs = require('fs'); const path = require('path');
const { ROOT, plan, D } = require('./helpers');
const share = require('../js/share.js'); const store = require('../js/store.js'); const L = require('../js/layout.js');
const R = require('../js/report.js'); const S = require('../js/samples.js'); const G = require('../js/glossary.js'); const M = require('../js/modal.js'); const T = require('../js/textutil.js');

test('share encode/decode round trip (compressed and plain fallback)', async () => {
  const obj = { v: 1, mode: 'analyze', text: fs.readFileSync(path.join(ROOT, 'samples/smj-aqe-off.txt'), 'utf8'), exec: null };
  const enc = await share.encode(obj); assert.match(enc, /^[zj][A-Za-z0-9_-]+$/); assert.deepEqual(await share.decode(enc), obj);
  const plain = 'j' + share.toB64u(new TextEncoder().encode(JSON.stringify({ v: 1, t: 'é✓' })));
  assert.deepEqual(await share.decode(plain), { v: 1, t: 'é✓' });
  if (enc[0] === 'z') assert.ok(enc.length < JSON.stringify(obj).length, 'compressed is smaller');
});
test('store: history add/update/remove/restore/clear with memory storage', () => {
  const m = new Map(); const s = store.create({ getItem: (k) => (m.has(k) ? m.get(k) : null), setItem: (k, v) => m.set(k, String(v)), removeItem: (k) => m.delete(k) });
  const a = s.history.add({ title: 'A' }); const b = s.history.add({ title: 'B' });
  assert.deepEqual(s.history.list().map((e) => e.title), ['B', 'A']);
  s.history.update(a.id, { title: 'A2' }); assert.equal(s.history.list()[1].title, 'A2');
  const r = s.history.remove(b.id); assert.equal(s.history.list().length, 1); s.history.restore(r.item, r.index); assert.equal(s.history.list()[0].id, b.id);
  const all = s.history.clear(); assert.equal(s.history.list().length, 0); s.history.restoreAll(all); assert.equal(s.history.list().length, 2);
  for (let i = 0; i < 70; i++) s.history.add({ title: 'x' + i }); assert.equal(s.history.list().length, 60);
  assert.ok([...m.keys()].every((k) => k.startsWith('spark-planreader:')));
  s.save({ overlay: false }); assert.deepEqual(s.load(), { overlay: false });
});
test('store falls back to memory when localStorage throws', () => {
  const s = store.create({ getItem() { throw new Error('x'); }, setItem() { throw new Error('quota'); }, removeItem() {} });
  s.history.add({ title: 'ok' }); assert.equal(s.history.list().length, 1);
});
test('layout: no overlapping nodes, edges reference laid-out nodes, plumbing hidden', () => {
  const files = fs.readdirSync(path.join(ROOT, 'samples')).filter((f) => f.endsWith('.txt'));
  for (const f of files) {
    const r = plan('samples/' + f); const l = L.layout(r.physical, { hidePlumbing: true });
    const ids = new Set(l.nodes.map((n) => n.id));
    l.edges.forEach((e) => { assert.ok(ids.has(e.from) && ids.has(e.to), f + ' edge'); });
    for (let i = 0; i < l.nodes.length; i++) for (let j = i + 1; j < l.nodes.length; j++) {
      const a = l.nodes[i], b = l.nodes[j]; const overlap = a.x < b.x + b.w && b.x < a.x + a.w && a.y < b.y + b.h && b.y < a.y + a.h;
      assert.ok(!overlap, f + ' overlap ' + a.node.name + ' / ' + b.node.name);
    }
    assert.ok(!l.nodes.some((n) => /^(InputAdapter|ColumnarToRow|WholeStageCodegen)/.test(n.node.name)), f + ' plumbing');
    assert.ok(l.width > 0 && l.height > 0);
  }
  const reuse = L.layout(plan('samples/reused-exchange.txt').physical, { hidePlumbing: true });
  assert.ok(reuse.edges.some((e) => e.kind === 'reuse')); assert.ok(reuse.stages.length >= 2);
});
test('reports: Markdown, standalone HTML and JSON', () => {
  const r = plan('samples/smj-aqe-off.txt'); const d = D.diagnose(r);
  const md = R.markdown(r, d, { title: 'Test plan' });
  assert.match(md, /^# Spark plan report: Test plan/m); assert.match(md, /SortMergeJoin/); assert.match(md, /```python/);
  const html = R.html(md, 'Test plan'); assert.match(html, /^<!doctype html>/i); assert.match(html, /<h1[^>]*>Spark plan report: Test plan<\/h1>/); assert.doesNotMatch(html, /<script/i);
  const j = JSON.parse(R.json(r, d)); assert.equal(j.findings.length, d.findings.length); assert.ok(JSON.stringify(j).length > 100);
  assert.equal(R.mdToHtml('<b>x</b>').includes('<b>'), false, 'escapes HTML');
});
test('samples index: every file exists and every fix pair resolves to files', () => {
  assert.ok(S.list.length >= 8);
  S.list.forEach((s) => assert.ok(fs.existsSync(path.join(ROOT, S.urlFor(s.id))), s.id));
  S.fixPairs.forEach((p) => { assert.ok(fs.existsSync(path.join(ROOT, S.urlFor(p.before))), p.before); assert.ok(fs.existsSync(path.join(ROOT, S.urlFor(p.after))), p.after); });
});
test('service worker caches every app file and sample', () => {
  const sw = fs.readFileSync(path.join(ROOT, 'sw.js'), 'utf8'); const html = fs.readFileSync(path.join(ROOT, 'index.html'), 'utf8');
  const scripts = [...html.matchAll(/<script src="([^"]+)"/g)].map((m) => './' + m[1]);
  scripts.concat(['./styles.css', './manifest.webmanifest']).forEach((s) => assert.ok(sw.includes("'" + s + "'"), 'sw caches ' + s));
  S.list.forEach((s) => assert.ok(sw.includes("'./" + S.urlFor(s.id) + "'"), 'sw caches sample ' + s.id));
  assert.doesNotMatch(sw + html, /src="\/|href="\//, 'paths are relative');
});
test('glossary covers common operators', () => {
  ['SortMergeJoin', 'BroadcastHashJoin', 'Exchange', 'HashAggregate', 'BatchEvalPython', 'FileScan parquet', 'AQEShuffleRead', 'Expand', 'Window'].forEach((n) => assert.ok(G.lookup(n), n));
  assert.ok(Object.keys(G.entries).length >= 60);
});
test('modal stack: Back closes the top sheet, dirty sheets ask first', () => {
  const states = [{}]; let idx = 0; const H = { get state() { return states[idx]; }, pushState(s) { states.splice(idx + 1); states.push(s); idx++; }, go(n) { idx += n; setTimeout(() => st.onPopState(), 0); } };
  let answer = false; const log = [];
  const st = M.createModalStack({ history: H, confirm: () => answer, setTimeout: (f) => f() });
  let dirty = true;
  st.open({ id: 'a', show: () => log.push('show a'), hide: () => log.push('hide a') });
  st.open({ id: 'b', isDirty: () => dirty, show: () => log.push('show b'), hide: () => log.push('hide b') });
  assert.equal(st.size, 2);
  idx--; st.onPopState(); assert.equal(st.size, 2, 'dirty sheet stays when user declines');
  answer = true; idx--; st.onPopState(); assert.equal(st.size, 1); assert.ok(log.includes('hide b'));
  dirty = false; return st.requestClose('a').then((ok) => { assert.equal(ok, true); assert.equal(st.size, 0); });
});
test('text utils: sizes and ids', () => {
  assert.equal(T.parseSize('62.5 KiB'), 64000); assert.equal(T.fmtBytes(1048576), '1.00 MiB');
  assert.equal(T.stripIds('customer_id#12L = id#3'), 'customer_id = id');
});
test('no visa wording anywhere in the app', () => {
  const files = ['index.html', 'README.md', ...fs.readdirSync(path.join(ROOT, 'js')).map((f) => 'js/' + f)].filter((f) => fs.existsSync(path.join(ROOT, f)));
  files.forEach((f) => assert.doesNotMatch(fs.readFileSync(path.join(ROOT, f), 'utf8'), /\b(F-1|visa|OPT)\b/, f));
});
