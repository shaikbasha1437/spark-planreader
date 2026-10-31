'use strict';
const test = require('node:test'); const assert = require('node:assert/strict');
const { E, D, read, find } = require('./helpers');
const text = read('samples/eventlog.json');
const ev = E.parseEventLog(text);

test('recognizes event logs and not plans', () => {
  assert.equal(E.looksLikeEventLog(text), true);
  assert.equal(E.looksLikeEventLog(read('samples/smj-aqe-off.txt')), false);
  assert.equal(E.looksLikeEventLog('{"a":1}'), false);
});
test('reads the app and both SQL executions with final AQE plans', () => {
  assert.equal(ev.app.sparkVersion, '4.0.1');
  assert.equal(ev.executions.length, 2);
  ev.executions.forEach((x) => { assert.ok(x.duration > 0); assert.ok(x.aqeUpdates >= 1); });
});
test('maps SQL metrics onto plan nodes with per-task spread', () => {
  const r = E.planFor(ev, 0);
  assert.equal(r.metrics, true); assert.equal(r.format, 'eventlog');
  const scans = r.nodes.filter((n) => n.category === 'scan' && n.metrics);
  assert.ok(scans.length >= 2);
  const rows = scans.map((n) => n.metrics['number of output rows']).filter(Boolean);
  assert.ok(rows.length && rows.every((m) => typeof m.value === 'number' && m.value > 0));
  const ex = r.nodes.find((n) => n.metrics && n.metrics['records read'] && n.metrics['records read'].tasks);
  assert.ok(ex, 'exchange with task-level records read'); const q = ex.metrics['records read'].tasks;
  assert.ok(q.count > 1 && q.max >= q.median && q.median >= q.min);
});
test('codegen wrappers are folded and InputAdapter dropped', () => {
  const r = E.planFor(ev, 0);
  assert.ok(!r.nodes.some((n) => n.name === 'InputAdapter'));
  assert.ok(!r.nodes.some((n) => /^WholeStageCodegen/.test(n.name)));
  assert.ok(r.nodes.some((n) => n.codegen));
});
test('event-log diagnostics: measured skew and broadcastable side', () => {
  const d0 = D.diagnose(E.planFor(ev, 0));
  const skew = find(d0, 'skew').find((f) => f.severity === 'high'); assert.ok(skew, 'high skew'); assert.match(skew.title, /\d+(\.\d+)?×/);
  const smj = find(d0, 'smj-broadcast')[0]; assert.equal(smj.severity, 'high'); assert.ok(smj.est && smj.est.bytes < 1024 * 1024);
  const d1 = D.diagnose(E.planFor(ev, 1));
  assert.ok(find(d1, 'python-udf').length); assert.ok(find(d1, 'small-files').length);
});
test('metric text parsing and quantiles', () => {
  const m = E.parseMetricText('total (min, med, max (stageId: taskId))\n9.2 MiB (1.1 MiB, 1.1 MiB, 1.3 MiB (stage 4.0: task 9))');
  assert.ok(m && m.value > 9 * 1048576 && m.value < 10 * 1048576);
  const q = E.quant([1, 2, 3, 4, 100]); assert.equal(q.max, 100); assert.equal(q.median, 3); assert.equal(q.count, 5); assert.equal(q.sum, 110);
});
test('broken lines and truncated logs do not throw', () => {
  const lines = text.split('\n'); const cut = lines.slice(0, Math.floor(lines.length * 0.6)).join('\n') + '\n{"Event":"SparkListenerTask';
  const p = E.parseEventLog(cut); assert.ok(p.notices.length >= 1); assert.doesNotThrow(() => p.executions.forEach((x) => E.planFor(p, x.id)));
});
