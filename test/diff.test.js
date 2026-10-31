'use strict';
const test = require('node:test'); const assert = require('node:assert/strict');
const { plan } = require('./helpers');
const X = require('../js/diff.js'); const S = require('../js/samples.js');
const pair = (a, b) => X.diffPlans(plan('samples/' + a + '.txt'), plan('samples/' + b + '.txt'));

test('broadcast fix removes two shuffles and switches the join', () => {
  const d = pair('smj-aqe-off', 'broadcast-fix');
  assert.equal(d.shuffles.before - d.shuffles.after, 2);
  assert.ok(d.joinChanges.some((c) => c.before && c.after && c.before.strategy === 'SortMergeJoin' && c.after.strategy === 'BroadcastHashJoin'));
  assert.ok(d.resolved.some((f) => f.rule === 'smj-broadcast')); assert.ok(d.score.after > d.score.before);
});
test('every fix pair improves the plan and resolves its finding', () => {
  const expect = { broadcast: 'smj-broadcast', native: 'python-udf', 'partition-filter': 'partition-pruning', partitioned: 'window-no-partition', approx: 'expand', typed: 'pushdown', 'no-coalesce': 'single-partition', salted: null };
  for (const p of S.fixPairs) {
    const d = pair(p.before, p.after);
    assert.ok(d.score.after >= d.score.before, p.id + ' score ' + d.score.before + ' → ' + d.score.after);
    assert.ok(d.summary.length > 0, p.id + ' summary');
    if (expect[p.id]) assert.ok(d.resolved.some((f) => f.rule === expect[p.id]), p.id + ' resolves ' + expect[p.id] + ' (resolved: ' + d.resolved.map((f) => f.rule) + ')');
  }
});
test('partition filter fix shows pruning added in scan changes', () => {
  const d = pair('partition-udf', 'fixes/partition_filter');
  assert.ok(d.scanChanges.some((s) => s.notes.some((n) => /partition pruning added/.test(n))));
  assert.ok(d.ops.some((o) => o.name === 'BatchEvalPython' && o.after === 0));
});
test('a plan compared with itself has no changes (ids ignored)', () => {
  const d = pair('smj-aqe-off', 'smj-aqe-off');
  assert.equal(d.ops.length, 0); assert.equal(d.joinChanges.length, 0); assert.ok(d.lines.every((l) => l.op === ' '));
});
test('line diff (LCS) basics', () => {
  const L = (arr) => arr.map((text) => ({ text }));
  const out = X.lineDiff(L(['a', 'b', 'c']), L(['a', 'x', 'c']));
  assert.deepEqual(out.map((o) => o.op), [' ', '-', '+', ' ']);
});
