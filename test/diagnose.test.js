'use strict';
const test = require('node:test'); const assert = require('node:assert/strict');
const { P, D, plan, diag, rules, find, sampleFiles } = require('./helpers');

const has = (rel, rule, sev) => { const f = find(diag(rel), rule); assert.ok(f.length, rel + ' should have ' + rule + ' (got ' + rules(diag(rel)).join(',') + ')'); if (sev) assert.ok(f.some((x) => x.severity === sev), rel + ' ' + rule + ' severity ' + f.map((x) => x.severity)); return f; };
const hasnt = (rel, rule) => assert.equal(find(diag(rel), rule).filter((f) => f.severity !== 'info').length, 0, rel + ' should not have ' + rule);

test('every finding has the full shape and a fix for non-info findings', () => {
  for (const f of sampleFiles()) {
    const d = diag(f);
    assert.ok(d.summary.score >= 0 && d.summary.score <= 100);
    d.findings.forEach((x) => {
      assert.ok(['high', 'medium', 'low', 'info'].includes(x.severity), f + ' severity');
      assert.ok(x.title && x.explain && x.why, f + ' ' + x.rule + ' text');
      assert.doesNotMatch(x.title, /#\d+/, f + ' title has raw expression ids: ' + x.title);
      if (x.severity !== 'info' && x.rule !== 'shuffles' && !(x.severity === 'low' && x.fix === null)) assert.ok(x.fix && (x.fix.pyspark || x.fix.sql || x.fix.config), f + ' ' + x.rule + ' fix');
    });
  }
});
test('score formula: 100 - 25 high - 10 medium - 3 low', () => {
  const d = diag('samples/partition-udf.txt'); const s = d.summary;
  assert.equal(s.score, Math.max(0, 100 - 25 * s.high - 10 * s.medium - 3 * s.low));
});
test('sort-merge join that could be broadcast', () => {
  has('samples/smj-aqe-off.txt', 'smj-broadcast', 'medium');
  hasnt('samples/broadcast-fix.txt', 'smj-broadcast');
  hasnt('samples/aqe-final-broadcast.txt', 'smj-broadcast');
});
test('nested loop / cartesian joins are high; tiny salting cross join is low', () => {
  has('samples/cartesian.txt', 'nested-loop', 'high'); has('samples/nested-loop-join.txt', 'nested-loop', 'high');
  const s = find(diag('samples/fixes/salted.txt'), 'nested-loop'); assert.ok(s.length && s.every((f) => f.severity === 'low'));
});
test('skew: AQE skew handling is reported as info on the final plan', () => {
  const f = has('samples/aqe-skew-final.txt', 'skew'); assert.ok(f.every((x) => x.severity === 'info'));
  hasnt('samples/smj-aqe-off.txt', 'skew');
});
test('partition pruning: UDF filter on the partition column is high, fixed version is clean', () => {
  has('samples/partition-udf.txt', 'partition-pruning', 'high');
  assert.equal(diag('samples/fixes/partition_filter.txt').findings.length, 0);
});
test('pushdown blocked by a cast on CSV and by a Python UDF', () => {
  has('samples/csv-cast.txt', 'pushdown', 'medium'); hasnt('samples/fixes/typed.txt', 'pushdown');
  has('samples/python-udf.txt', 'pushdown');
  hasnt('samples/explode-collect.txt', 'pushdown'); // inferred size(x) > 0 filters are not flagged
});
test('Python UDFs: BatchEvalPython high, Arrow (pandas) low', () => {
  has('samples/python-udf.txt', 'python-udf', 'high'); has('samples/pandas-udf.txt', 'python-udf', 'low');
  hasnt('samples/fixes/native.txt', 'python-udf');
});
test('window without PARTITION BY', () => { has('samples/window-no-partition.txt', 'window-no-partition', 'high'); hasnt('samples/fixes/partitioned.txt', 'window-no-partition'); });
test('count(DISTINCT) Expand', () => { has('samples/count-distinct.txt', 'expand', 'medium'); hasnt('samples/fixes/approx.txt', 'expand'); });
test('coalesce(1) and repartition(1) single partition', () => {
  has('samples/sort-coalesce.txt', 'single-partition', 'high'); has('samples/repartition-one.txt', 'single-partition', 'high');
  hasnt('samples/fixes/no_coalesce.txt', 'single-partition');
});
test('global sort', () => { has('samples/smj-aqe-off.txt', 'global-sort', 'medium'); hasnt('samples/aqe-final-broadcast.txt', 'global-sort'); });
test('AQE off is flagged only when the plan has no AdaptiveSparkPlan', () => { has('samples/smj-aqe-off.txt', 'aqe-off', 'medium'); hasnt('samples/aqe-skew-final.txt', 'aqe-off'); });
test('collect_list + explode', () => { has('samples/explode-collect.txt', 'collect-explode', 'medium'); hasnt('samples/smj-aqe-off.txt', 'collect-explode'); });
test('text formats (CSV/JSON)', () => { has('samples/csv-cast.txt', 'text-format', 'medium'); hasnt('samples/smj-aqe-off.txt', 'text-format'); });
test('repeated scans of the same table', () => { has('samples/subquery-dpp.txt', 'repeated-scan'); hasnt('samples/smj-aqe-off.txt', 'repeated-scan'); hasnt('test/fixtures/codegen.txt', 'repeated-scan'); });
test('Photon fallback', () => { has('samples/photon-illustrative.txt', 'photon-fallback', 'medium'); hasnt('samples/smj-aqe-off.txt', 'photon-fallback'); });
test('wide ReadSchema', () => {
  const cols = Array.from({ length: 25 }, (_, i) => 'c' + i);
  const t = '== Physical Plan ==\n*(1) ColumnarToRow\n+- FileScan parquet default.wide[' + cols.map((c, i) => c + '#' + i).join(',') + '] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/data/wide], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<' + cols.map((c) => c + ':int').join(',') + '>\n';
  const d = D.diagnose(P.parsePlan(t)); const f = find(d, 'wide-schema'); assert.equal(f.length, 1); assert.match(f[0].title, /25 columns/);
  hasnt('samples/smj-aqe-off.txt', 'wide-schema');
});
test('spill and big broadcast use metrics', () => {
  const r = plan('samples/aqe-final-broadcast.txt');
  const sort = r.nodes.find((n) => /Aggregate|Sort/.test(n.name)); sort.metrics = { 'spill size': { value: 2 * 1024 ** 3, type: 'size', tasks: { max: 1024 ** 3 } } };
  const b = r.nodes.find((n) => n.category === 'broadcast'); b.metrics = { 'data size': { value: 512 * 1024 ** 2, type: 'size' } };
  const d = D.diagnose(r);
  assert.equal(find(d, 'spill')[0].severity, 'high'); assert.ok(find(d, 'big-broadcast').length);
  const clean = D.diagnose(plan('samples/aqe-final-broadcast.txt')); assert.equal(find(clean, 'spill').length + find(clean, 'big-broadcast').length, 0);
});
test('broadcast threshold from conf changes the explanation', () => {
  const r = plan('samples/cost-stats.txt'); r.conf = { 'spark.sql.autoBroadcastJoinThreshold': '-1' };
  assert.doesNotThrow(() => D.diagnose(r));
});
test('findings sorted by severity and have unique ids', () => {
  const order = { high: 0, medium: 1, low: 2, info: 3 };
  for (const f of sampleFiles()) { const d = diag(f); const s = d.findings.map((x) => order[x.severity]); assert.deepEqual(s, s.slice().sort((a, b) => a - b), f); assert.equal(new Set(d.findings.map((x) => x.id)).size, d.findings.length); }
});
test('diagnose handles an unparseable plan', () => { const d = D.diagnose(P.parsePlan('nope')); assert.equal(d.findings.length, 0); });
