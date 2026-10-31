/* Spark PlanReader event-log reader: Spark event logs (JSON lines), single SQL execution events and the
   Spark UI REST /sql JSON. Rebuilds each SQL execution's final plan from sparkPlanInfo and attaches the
   SQL metrics (rows, sizes, times, spill) with a per-task spread so skew shows up. */
(function (root, factory) {
  const m = factory(root.SP ? root.SP.parser : require('./parser.js'), root.SP ? root.SP.text : require('./textutil.js'));
  if (typeof module === 'object' && module.exports) module.exports = m;
  else (root.SP = root.SP || {}).eventlog = m;
})(typeof self !== 'undefined' ? self : this, function (P, T) {
  'use strict';
  function looksLikeEventLog(text) {
    const s = String(text || '').trimStart();
    if (!(s.startsWith('{') || s.startsWith('['))) return false;
    return /"Event"\s*:\s*"/.test(s.slice(0, 200000)) || /"sparkPlanInfo"/.test(s.slice(0, 200000)) || /"planDescription"/.test(s.slice(0, 200000));
  }
  const short = (ev) => String(ev || '').split('.').pop();
  function quant(arr) {
    if (!arr || !arr.length) return null;
    const a = arr.slice().sort((x, y) => x - y); const q = (p) => a[Math.min(a.length - 1, Math.floor(p * (a.length - 1) + 0.5))];
    return { count: a.length, min: a[0], p25: q(0.25), median: q(0.5), p75: q(0.75), max: a[a.length - 1], sum: a.reduce((s, x) => s + x, 0) };
  }
  // ---------- event log (JSON lines) ----------
  function parseEventLog(text) {
    const notices = []; const execs = new Map(); const accTotals = new Map(); const accStage = new Map(); const accTasks = new Map(); const accDriver = new Map();
    const app = { sparkVersion: null, name: null, props: {} };
    let bad = 0, n = 0;
    let events = [];
    const s = String(text || '').trim();
    if (s.startsWith('[')) { try { events = JSON.parse(s); } catch (e) { notices.push({ level: 'warn', text: 'The JSON array could not be read as a whole; trying line by line.' }); } }
    if (!events.length) {
      s.split(/\n/).forEach((line) => { const l = line.trim(); if (!l) return; try { events.push(JSON.parse(l)); } catch (e) { bad++; } });
    }
    if (events.length === 1 && events[0] && !events[0].Event && (events[0].planDescription || Array.isArray(events[0].nodes))) return parseRestSql(events, notices);
    if (events.length && !events[0].Event && events.every((e) => e && (e.planDescription || e.nodes))) return parseRestSql(events, notices);
    events.forEach((e) => {
      if (!e || typeof e !== 'object') return; n++;
      const ev = short(e.Event);
      switch (ev) {
        case 'SparkListenerLogStart': app.sparkVersion = e['Spark Version'] || app.sparkVersion; break;
        case 'SparkListenerApplicationStart': app.name = e['App Name'] || app.name; break;
        case 'SparkListenerEnvironmentUpdate': Object.assign(app.props, e['Spark Properties'] || {}); break;
        case 'SparkListenerSQLExecutionStart': {
          const x = getExec(execs, e.executionId); x.description = e.description || ''; x.start = e.time; x.planText = e.physicalPlanDescription || ''; x.initialInfo = e.sparkPlanInfo; x.info = e.sparkPlanInfo; x.modifiedConfigs = e.modifiedConfigs || {}; x.rootExecutionId = e.rootExecutionId; break;
        }
        case 'SparkListenerSQLAdaptiveExecutionUpdate': { const x = getExec(execs, e.executionId); x.info = e.sparkPlanInfo || x.info; x.planText = e.physicalPlanDescription || x.planText; x.aqeUpdates = (x.aqeUpdates || 0) + 1; break; }
        case 'SparkListenerSQLAdaptiveSQLMetricUpdates': break;
        case 'SparkListenerSQLExecutionEnd': { const x = getExec(execs, e.executionId); x.end = e.time; x.error = e.errorMessage || ''; break; }
        case 'SparkListenerDriverAccumUpdates': (e.accumUpdates || []).forEach(([id, v]) => accDriver.set(id, (accDriver.get(id) || 0) + Number(v))); break;
        case 'SparkListenerStageCompleted': {
          const si = e['Stage Info'] || {}; const key = si['Stage ID'] + '.' + (si['Stage Attempt ID'] || 0);
          (si.Accumulables || []).forEach((a) => { if (a.Value == null) return; const v = Number(a.Value); if (!isFinite(v)) return; let m = accStage.get(a.ID); if (!m) accStage.set(a.ID, (m = new Map())); m.set(key, v); });
          break;
        }
        case 'SparkListenerTaskEnd': {
          const reason = e['Task End Reason'] && e['Task End Reason'].Reason; if (reason && reason !== 'Success') break;
          const ti = e['Task Info'] || {};
          (ti.Accumulables || []).forEach((a) => { if (a.Update == null) return; const v = Number(a.Update); if (!isFinite(v)) return; let arr = accTasks.get(a.ID); if (!arr) accTasks.set(a.ID, (arr = [])); arr.push(v); });
          break;
        }
        default: break;
      }
    });
    if (bad) notices.push({ level: 'warn', text: bad + ' line' + (bad > 1 ? 's' : '') + ' could not be read as JSON and ' + (bad > 1 ? 'were' : 'was') + ' skipped (a cut-off copy is fine; the rest is still used).' });
    accStage.forEach((m, id) => { let t = 0; m.forEach((v) => { t += v; }); accTotals.set(id, t); });
    accDriver.forEach((v, id) => accTotals.set(id, (accTotals.get(id) || 0) + v));
    accTasks.forEach((arr, id) => { if (!accTotals.has(id)) accTotals.set(id, arr.reduce((s, x) => s + x, 0)); });
    const metricsCtx = { totals: accTotals, tasks: accTasks };
    const list = [...execs.values()].filter((x) => x.info || x.planText).sort((a, b) => a.id - b.id);
    if (!n) notices.push({ level: 'error', text: 'No Spark events found in this file.' });
    else if (!list.length) notices.push({ level: 'error', text: 'This event log has no SQL executions (no SparkListenerSQLExecutionStart events). Only DataFrame / SQL jobs have plans.' });
    list.forEach((x) => { x.duration = x.end && x.start ? x.end - x.start : null; });
    return { input: 'eventlog', app, executions: list.map((x) => ({ id: x.id, description: x.description, start: x.start, end: x.end, duration: x.duration, error: x.error, aqeUpdates: x.aqeUpdates || 0, nodes: countNodes(x.info) })), notices, _execs: list, _metrics: metricsCtx, eventCount: n };
  }
  function getExec(map, id) { if (!map.has(id)) map.set(id, { id }); return map.get(id); }
  function countNodes(info) { let c = 0; const go = (i) => { if (!i) return; c++; (i.children || []).forEach(go); }; go(info); return c; }
  // Convert sparkPlanInfo to parser nodes. WholeStageCodegen / InputAdapter wrappers become codegen marks.
  function convert(info, codegen, ctx) {
    if (!info) return [];
    const name = info.nodeName || ''; const kids = info.children || [];
    const wm = /^WholeStageCodegen\s*\((\d+)\)/.exec(name);
    if (wm) { attachExtra(ctx, info, Number(wm[1])); return kids.flatMap((c) => convert(c, Number(wm[1]), ctx)); }
    if (name === 'InputAdapter') return kids.flatMap((c) => convert(c, null, ctx));
    const text = (codegen ? '*(' + codegen + ') ' : '') + (info.simpleString || name);
    const node = P.makeNode(text, 0, false);
    if (!node.name || node.name === '?') node.name = name;
    node.metrics = readMetrics(info.metrics, ctx);
    if (name === 'ReusedExchange' || /^Reused/.test(name)) { node.children = []; return [node]; }
    node.children = kids.flatMap((c) => convert(c, /QueryStage|^Subquery|^AdaptiveSparkPlan/.test(name) ? null : null, ctx));
    if (ctx.pendingCodegen && ctx.pendingCodegen[codegen] && !node._cg) { /* codegen duration lives on the wrapper */ }
    return [node];
  }
  function attachExtra(ctx, info, id) { const m = readMetrics(info.metrics, ctx); if (m && Object.keys(m).length) (ctx.codegenMetrics = ctx.codegenMetrics || {})[id] = m; }
  function readMetrics(list, ctx) {
    if (!list || !list.length) return null; const out = {};
    list.forEach((m) => {
      const id = m.accumulatorId; const total = ctx.totals.get(id); const tasks = ctx.tasks.get(id);
      if (total == null && !tasks) return;
      const type = m.metricType || 'sum';
      const conv = (v) => (type === 'nsTiming' ? v / 1e6 : v);
      const q = quant(tasks ? tasks.map(conv) : null);
      out[m.name] = { value: conv(total != null ? total : 0), type: type === 'nsTiming' ? 'timing' : type, tasks: q };
    });
    return Object.keys(out).length ? out : null;
  }
  function planFromExecution(parsed, id) {
    const x = parsed._execs.find((e) => e.id === id) || parsed._execs[0];
    if (!x) return P.finish({ input: 'eventlog', format: 'eventlog', sections: [], physical: null, logical: {}, subqueries: [], notices: parsed.notices.slice(), aqe: { present: false, final: false }, version: parsed.app.sparkVersion ? 'Spark ' + parsed.app.sparkVersion : null });
    P.resetSeq(0);
    const ctx = Object.assign({}, parsed._metrics);
    const roots = convert(x.info, null, ctx);
    let root = roots[0] || null;
    if (roots.length > 1) { root = P.makeNode('Plans', 0, false); root.children = roots; }
    if (root && ctx.codegenMetrics) P.walk(root, (n) => { if (n.codegen != null && ctx.codegenMetrics[n.codegen] && !n.codegenMetrics) { n.codegenMetrics = ctx.codegenMetrics[n.codegen]; } });
    let initial = null;
    if (x.initialInfo && x.initialInfo !== x.info) { const r = convert(x.initialInfo, null, { totals: new Map(), tasks: new Map() }); initial = r[0] || null; if (initial) P.walk(initial, (n) => { n.metrics = null; }); }
    const notices = parsed.notices.filter((n) => n.level !== 'error').slice();
    const props = Object.assign({}, parsed.app.props, x.modifiedConfigs || {});
    const res = { input: 'eventlog', format: 'eventlog', sections: [{ title: 'Physical Plan (' + (x.aqeUpdates ? 'final, after AQE' : 'as executed') + ')', kind: 'physical', text: (x.planText || '').replace(/^== Physical Plan ==\n/, '').trim() }], physical: root, initialPhysical: initial, logical: {}, subqueries: [], notices, photon: null,
      version: parsed.app.sparkVersion ? 'Spark ' + parsed.app.sparkVersion : P.detectVersion(x.planText || ''), aqe: { present: false, final: !!x.aqeUpdates }, metrics: true,
      execution: { id: x.id, description: x.description, duration: x.duration, error: x.error, start: x.start }, conf: props };
    if (root) P.walk(root, (n) => { if (n.name === 'AdaptiveSparkPlan') { res.aqe.present = true; if (n.attrs.isFinalPlan) res.aqe.final = true; } });
    if (x.error) notices.push({ level: 'warn', text: 'This execution failed: ' + String(x.error).split('\n')[0].slice(0, 300) });
    if (!x.end) notices.push({ level: 'info', text: 'This execution has no end event (still running when the log was copied, or the log is cut off); metrics may be partial.' });
    return P.finish(res);
  }
  // ---------- Spark UI REST API: /api/v1/applications/<app>/sql?details=true ----------
  function parseMetricText(v) {
    const s = String(v == null ? '' : v);
    // "total (min, med, max (stageId: taskId))\n9.2 MiB (1.1 MiB, 1.1 MiB, 1.3 MiB (stage 4.0: task 9))"
    const lines = s.split('\n'); const body = lines.length > 1 ? lines[lines.length - 1] : s;
    const m = /^([^(]+?)\s*\(([^,]+),\s*([^,]+),\s*([^(]+?)\s*(?:\(.*\))?\)\s*$/.exec(body.trim());
    const num = (x) => { const b = T.parseSize(x); if (b != null) return { v: b, type: 'size' }; const t = /^([\d.,]+)\s*(ms|s|m|h)$/.exec(x.trim()); if (t) { const k = { ms: 1, s: 1000, m: 60000, h: 3600000 }[t[2]]; return { v: Number(t[1].replace(/,/g, '')) * k, type: 'timing' }; } const n = Number(String(x).replace(/,/g, '')); return isFinite(n) ? { v: n, type: 'sum' } : null; };
    if (m) { const t = num(m[1]); const mn = num(m[2]), md = num(m[3]), mx = num(m[4]); if (t) return { value: t.v, type: t.type, tasks: mn && md && mx ? { count: null, min: mn.v, median: md.v, max: mx.v } : null }; }
    const single = num(body); return single ? { value: single.v, type: single.type, tasks: null } : { value: null, type: 'text', text: s, tasks: null };
  }
  function parseRestSql(list, notices) {
    const execs = list.map((e, i) => ({ id: e.id != null ? e.id : i, description: e.description || '', duration: e.duration || null, start: e.submissionTime ? Date.parse(e.submissionTime) : null, error: e.status === 'FAILED' ? 'FAILED' : '', planText: e.planDescription || '', rest: e }));
    const parsed = { input: 'eventlog', app: { sparkVersion: null, props: {} }, executions: execs.map((x) => ({ id: x.id, description: x.description, duration: x.duration, error: x.error, nodes: (x.rest.nodes || []).length })), notices, _execs: execs, rest: true, eventCount: list.length };
    parsed.planFor = (id) => restPlan(parsed, id);
    return parsed;
  }
  function restPlan(parsed, id) {
    const x = parsed._execs.find((e) => e.id === id) || parsed._execs[0];
    const res = P.parsePlan(x.planText || '');
    res.input = 'eventlog'; res.format = 'rest'; res.metrics = true; res.execution = { id: x.id, description: x.description, duration: x.duration, error: x.error };
    // Match REST nodes to plan nodes by operator id when the REST node names carry "(n)", else by name order.
    const restNodes = (x.rest.nodes || []).filter((n) => !/^WholeStageCodegen|^InputAdapter/.test(n.nodeName));
    const used = new Set();
    res.nodes.forEach((n) => {
      const cand = restNodes.find((r) => !used.has(r) && r.nodeName.trim() === n.name.trim()) || restNodes.find((r) => !used.has(r) && n.name.startsWith(r.nodeName.trim().split(' ')[0]));
      if (!cand) return; used.add(cand);
      const m = {}; (cand.metrics || []).forEach((mm) => { m[mm.name] = parseMetricText(mm.value); }); if (Object.keys(m).length) n.metrics = m;
    });
    res.notices.push({ level: 'info', text: 'Read from the Spark UI REST API: metrics are matched to plan operators by name and order.' });
    return res;
  }
  function planFor(parsed, id) { return parsed.rest ? restPlan(parsed, id) : planFromExecution(parsed, id); }
  // The most interesting execution: the longest one with at least one exchange or join.
  function defaultExecution(parsed) {
    const ex = parsed.executions.filter((e) => e.nodes > 2);
    const pick = (ex.length ? ex : parsed.executions).slice().sort((a, b) => (b.duration || 0) - (a.duration || 0) || b.nodes - a.nodes)[0];
    return pick ? pick.id : null;
  }
  return { looksLikeEventLog, parseEventLog, planFor, defaultExecution, parseMetricText, quant };
});
