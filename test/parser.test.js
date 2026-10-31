'use strict';
const test = require('node:test'); const assert = require('node:assert/strict');
const { P, read, plan, sampleFiles } = require('./helpers');

test('every sample parses into a physical tree with no error notices', () => {
  const files = sampleFiles(); assert.ok(files.length >= 30, 'expected 30+ sample files, got ' + files.length);
  for (const f of files) {
    const r = plan(f);
    assert.ok(r.ok, f + ' should parse'); assert.ok(r.physical, f + ' physical');
    assert.ok(r.nodes.length >= 2, f + ' nodes');
    assert.equal(r.notices.filter((n) => n.level === 'error').length, 0, f + ' has error notices');
    r.nodes.forEach((n) => assert.ok(n.id && n.name && Array.isArray(n.children), f + ' node shape'));
  }
});
test('detects explain formats', () => {
  assert.equal(plan('samples/smj-aqe-off.txt').format, 'simple');
  assert.equal(plan('samples/star-join-extended.txt').format, 'extended');
  assert.equal(plan('samples/star-join-formatted.txt').format, 'formatted');
  assert.equal(plan('samples/cost-stats.txt').format, 'cost');
  assert.equal(plan('test/fixtures/codegen.txt').format, 'codegen');
});
test('extended mode keeps logical sections', () => {
  const r = plan('samples/star-join-extended.txt');
  const kinds = r.sections.map((s) => s.kind);
  assert.ok(kinds.includes('physical')); assert.equal(kinds.filter((k) => k === 'logical').length, 3, kinds.join());
  assert.deepEqual(r.sections.slice(0, 3).map((s) => s.title.split(' ')[0]), ['Parsed', 'Analyzed', 'Optimized']);
});
test('formatted mode attaches node details by operator id', () => {
  const r = plan('samples/star-join-formatted.txt');
  const scan = r.nodes.find((n) => n.category === 'scan');
  assert.ok(scan.details && scan.details.length > 0, 'scan has details');
  assert.ok((scan.attrs.readCols || []).length > 0, 'readCols from formatted details');
});
test('formatted mode with scalar subqueries', () => {
  const r = plan('test/fixtures/formatted-subquery.txt');
  assert.ok(r.ok);
  assert.ok(r.nodes.filter((n) => n.category === 'subquery').length >= 2, 'two subqueries');
});
test('codegen markers: *(n) ids assigned and codegen mode subtrees', () => {
  const r = plan('samples/smj-aqe-off.txt');
  const ids = new Set(r.nodes.map((n) => n.codegen).filter(Boolean));
  assert.ok(ids.size >= 5, 'codegen ids ' + [...ids]);
  const c = plan('test/fixtures/codegen.txt');
  assert.ok(new Set(c.nodes.map((n) => n.codegen).filter(Boolean)).size >= 3);
  assert.ok(c.notices.some((n) => /codegen/i.test(n.text)));
});
test('AQE: final flag, initial plan and query stages', () => {
  const r = plan('samples/aqe-skew-final.txt');
  assert.equal(r.aqe.present, true); assert.equal(r.aqe.final, true);
  assert.ok(r.initialPhysical, 'initial plan parsed');
  assert.ok(r.nodes.some((n) => /QueryStage|AQEShuffleRead/.test(n.name)));
  const off = plan('samples/smj-aqe-off.txt'); assert.equal(off.aqe.present, false);
});
test('ReusedExchange links to the original exchange', () => {
  const r = plan('samples/reused-exchange.txt');
  const reused = r.nodes.filter((n) => n.name === 'ReusedExchange');
  assert.ok(reused.length >= 1);
  reused.forEach((n) => { assert.ok(n.reuseOf, 'reuseOf set'); assert.ok(r.nodes.some((m) => m.id === n.reuseOf && /Exchange/.test(m.name))); });
});
test('union and subquery/DPP plans', () => {
  const u = plan('test/fixtures/union.txt'); assert.ok(u.nodes.some((n) => n.name === 'Union'));
  assert.equal(u.nodes.find((n) => n.name === 'Union').children.length, 2);
  const s = plan('samples/subquery-dpp.txt'); assert.ok(s.nodes.some((n) => /Subquery|dynamicpruning/i.test(n.name + n.text)));
});
test('Photon plans are recognized', () => {
  const r = plan('samples/photon-illustrative.txt');
  assert.ok(r.nodes.some((n) => P.isPhoton(n.name)));
  assert.match(r.version || '', /Photon/);
});
test('Spark version heuristics: 4.x for 4.0 plans, 3.5 samples parse the same', () => {
  assert.match(plan('samples/aqe-skew-final.txt').version || '', /4/);
  const a = plan('samples/spark35-aqe-skew-final.txt'); assert.ok(a.ok && a.aqe.final);
});
test('stages: exchanges start new stages', () => {
  const r = plan('samples/smj-aqe-off.txt');
  const stages = new Set(r.nodes.map((n) => n.stage)); assert.ok(stages.size >= 4, 'stages ' + stages.size);
  // An exchange belongs to its map side; the operator reading it is in the next stage.
  const ex = r.nodes.find((n) => P.isShuffle(n)); const parent = r.nodes.find((n) => n.children.includes(ex));
  assert.equal(ex.stage, ex.children[0].stage); assert.notEqual(ex.stage, parent.stage);
});
test('scan parts: format, table, partition and pushed filters', () => {
  const r = plan('samples/csv-cast.txt'); const s = r.nodes.find((n) => n.category === 'scan');
  assert.match(s.attrs.format || s.name, /csv/i);
  const p = plan('samples/fixes/partition_filter.txt').nodes.find((n) => n.category === 'scan');
  assert.ok(p.attrs.partitionFilters.length > 0);
});
test('accepts plans pasted with pipe table borders', () => {
  const raw = read('samples/smj-aqe-off.txt').split('\n').filter((l) => l.trim());
  const w = Math.max(...raw.map((l) => l.length));
  const boxed = '+' + '-'.repeat(w + 2) + '+\n|plan|\n+' + '-'.repeat(w + 2) + '+\n' + raw.map((l) => '|' + l.padEnd(w) + '|').join('\n') + '\n+' + '-'.repeat(w + 2) + '+';
  const r = P.parsePlan(boxed);
  assert.ok(r.ok); assert.equal(r.nodes.length, plan('samples/smj-aqe-off.txt').nodes.length);
});
test('accepts escaped \\n plans (copied from a notebook or JSON string)', () => {
  const t = read('samples/smj-aqe-off.txt'); const esc = JSON.stringify(t).slice(1, -1);
  const r = P.parsePlan(esc); assert.ok(r.ok); assert.equal(r.nodes.length, plan('samples/smj-aqe-off.txt').nodes.length);
});
test('garbage input returns a friendly error instead of throwing', () => {
  for (const t of ['', 'hello world', 'SELECT * FROM t', '{"a":1}', '+- +- :- ', '\u0000\u0001']) {
    const r = P.parsePlan(t); assert.equal(r.ok, false, JSON.stringify(t)); assert.ok(r.notices.length > 0);
  }
});
test('partial / truncated plans still parse what is there', () => {
  const lines = read('samples/smj-aqe-off.txt').split('\n'); const half = lines.slice(0, Math.ceil(lines.length / 2)).join('\n');
  const r = P.parsePlan(half); assert.ok(r.ok); assert.ok(r.nodes.length >= 4);
});
test('plan without a header line (just the tree) parses', () => {
  const t = read('samples/smj-aqe-off.txt').replace(/^== Physical Plan ==\n/, '');
  assert.ok(P.parsePlan(t).ok);
});
