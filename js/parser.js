/* Spark PlanReader parser: turns EXPLAIN text (simple, extended, formatted, cost, codegen; Spark 3.x/4.x,
   AQE, Photon) into a tree of operator nodes with normalized attributes. Never throws on odd input:
   whatever it can't read becomes a notice and the rest is still returned. */
(function (root, factory) {
  const m = factory(root.SP ? root.SP.text : require('./textutil.js'));
  if (typeof module === 'object' && module.exports) module.exports = m;
  else (root.SP = root.SP || {}).parser = m;
})(typeof self !== 'undefined' ? self : this, function (T) {
  'use strict';
  const { splitTop, matchClose, parseList, leadingLists, keyValues, parseSize, schemaCols, attrName } = T;

  // ---------- operator taxonomy ----------
  const EXCHANGE_RE = /^(Exchange|ShuffleExchange|ColumnarExchange|GpuColumnarExchange|PhotonShuffleExchangeSink|BroadcastExchange|PhotonBroadcastExchange|ColumnarBroadcastExchange|ReusedExchange)$/;
  function categoryOf(name) {
    const n = name || '';
    if (/^== /.test(n)) return 'marker';
    if (/^(Subquery|SubqueryBroadcast|SubqueryAdaptiveBroadcast|ReusedSubquery|PhotonSubquery)/.test(n)) return 'subquery';
    if (/^(InMemoryTableScan|InMemoryRelation|TableCacheQueryStage)/.test(n)) return 'cache';
    if (/^(BroadcastExchange|PhotonBroadcastExchange|ColumnarBroadcastExchange)$/.test(n)) return 'broadcast';
    if (/Exchange|PhotonShuffleExchangeSource/.test(n)) return 'exchange';
    if (/Join$|^CartesianProduct|JoinExec|^PhotonBroadcastHashJoin|^Join$/.test(n)) return 'join';
    if (/(EvalPython|InPandas|InArrow|PythonUDTF|PythonRunner|ArrowEvalPythonUDTF|BatchEvalPythonUDTF|ArrowAggregatePython|ArrowWindowPython|AggregateInPandas|WindowInPandas)/.test(n)) return 'udf';
    if (/Scan|^Relation$|^LocalRelation$|^RDDScan|^LocalTableScan|^ExternalRDD|^Range$|^OneRowRelation/.test(n)) return 'scan';
    if (/Aggregate|GroupingAgg|^PhotonAgg/.test(n)) return 'agg';
    if (/^Window|PhotonWindow/.test(n)) return 'window';
    if (/^(Sort|PhotonSort|TakeOrderedAndProject|PhotonTopK)/.test(n)) return 'sort';
    if (/^(Project|PhotonProject)$/.test(n)) return 'project';
    if (/^(Filter|PhotonFilter)$/.test(n)) return 'filter';
    if (/^(Generate|Expand|PhotonGenerate|PhotonExpand)$/.test(n)) return 'generate';
    if (/^(Union|PhotonUnion)/.test(n)) return 'union';
    if (/Limit|^Coalesce$|^Sample$/.test(n)) return 'limit';
    if (/^Execute |Command|^WriteFiles|^Insert|^AppendData|^OverwriteByExpression|^AtomicReplaceTableAsSelect|^WriteToDataSourceV2|^PhotonWrite/.test(n)) return 'write';
    if (/QueryStage|AQEShuffleRead|CustomShuffleReader|^AdaptiveSparkPlan|^WholeStageCodegen|^InputAdapter|ColumnarToRow|RowToColumnar|^PhotonResultStage|^PhotonShuffleMapStage|^PhotonAdapter/.test(n)) return 'stage';
    return 'other';
  }
  const isExchange = (n) => EXCHANGE_RE.test(n.name);
  const isShuffle = (n) => /^(Exchange|ShuffleExchange|ColumnarExchange|GpuColumnarExchange|PhotonShuffleExchangeSink)$/.test(n.name) && !(n.attrs.partitioning && n.attrs.partitioning.type === 'broadcast');
  const isPhoton = (name) => /^Photon/.test(name || '');

  // ---------- input clean-up ----------
  function normalize(text) {
    let s = String(text == null ? '' : text).replace(/\r\n?/g, '\n').replace(/\u00a0/g, ' ');
    // A Python repr or a JSON string: "== Physical Plan ==\n..." with escaped newlines.
    if (!/\n/.test(s.trim()) && /\\n/.test(s)) s = s.replace(/^\s*['"]|['"]\s*$/g, '').replace(/\\n/g, '\n').replace(/\\t/g, '\t').replace(/\\"/g, '"');
    let lines = s.split('\n');
    // DataFrame.show() borders: "|== Physical Plan ==   |"
    const piped = lines.filter((l) => l.trim()).filter((l) => /^\s*\|/.test(l) || /^\s*\+-{3,}/.test(l));
    if (piped.length && piped.length >= lines.filter((l) => l.trim()).length * 0.8) {
      lines = lines.filter((l) => !/^\s*\+-{3,}/.test(l) && !/^\s*\|\s*plan\s*\|\s*$/i.test(l)).map((l) => l.replace(/^\s*\|/, '').replace(/\s*\|\s*$/, ''));
    }
    lines = lines.map((l) => l.replace(/\s+$/, ''));
    // Common leading indentation (pasted from a notebook cell or a log line).
    const ind = lines.filter((l) => l.trim()).map((l) => /^ */.exec(l)[0].length);
    const min = ind.length ? Math.min(...ind) : 0;
    if (min > 0) lines = lines.map((l) => l.slice(min));
    return lines;
  }

  // ---------- node text -> name / args ----------
  const FORMATTED_LINE = /^(\*\s)?(.*?)\s*\((\d+)\)(?:,\s*(Statistics\(.*\)))?\s*$/;
  function splitStats(text) {
    const m = /,?\s*Statistics\((.*)\)\s*$/.exec(text);
    if (!m) return { text, stats: null };
    const stats = {}; splitTop(m[1], ',').forEach((p) => { const [k, v] = p.split('='); if (!v) return; stats[k.trim()] = v.trim(); });
    const out = { raw: m[1] };
    if (stats.sizeInBytes) out.sizeInBytes = parseSize(stats.sizeInBytes) != null ? parseSize(stats.sizeInBytes) : Number(stats.sizeInBytes);
    if (stats.rowCount) out.rowCount = Number(stats.rowCount);
    return { text: text.slice(0, m.index).trim(), stats: out };
  }
  function nameOf(text) {
    let t = text.trim();
    let m;
    if ((m = /^==\s*(.+?)\s*==$/.exec(t))) return { name: '== ' + m[1] + ' ==', rest: '' };
    if ((m = /^(FileScan|Scan|PhotonScan|ColumnarScan|GpuScan)\s+([A-Za-z][\w-]*)(.*)$/s.exec(t))) return { name: m[1] + ' ' + m[2], rest: m[3].trim(), format: m[2] };
    if ((m = /^(Execute)\s+(\w+)(.*)$/s.exec(t))) return { name: 'Execute ' + m[2], rest: m[3].trim() };
    if ((m = /^'?([A-Za-z_][\w$]*)(.*)$/s.exec(t))) {
      let rest = m[2]; let name = m[1]; const flags = {};
      const sk = /^\(skew=(true|false)\)/.exec(rest); if (sk) { flags.skew = sk[1] === 'true'; rest = rest.slice(sk[0].length); }
      return { name, rest: rest.trim(), flags, unresolved: t.startsWith("'") };
    }
    return { name: t.split(/\s+/)[0] || '?', rest: t.slice((t.split(/\s+/)[0] || '').length).trim() };
  }
  function namedArgs(s) { // "(keys=[a], functions=[b], output=[c])" -> {keys, functions, output}
    const out = {}; if (!s || s[0] !== '(') return out;
    const end = matchClose(s, 0); const inner = end > 0 ? s.slice(1, end) : s.slice(1);
    splitTop(inner, ',').forEach((p) => { const i = p.indexOf('='); if (i > 0) out[p.slice(0, i).trim()] = p.slice(i + 1).trim(); });
    return out;
  }
  function parsePartitioning(s) {
    s = (s || '').trim(); let m;
    if ((m = /^(hashpartitioning|rangepartitioning|RoundRobinPartitioning|hashclusteredpartitioning|KeyGroupedPartitioning)\((.*)$/is.exec(s))) {
      const end = matchClose(s, m[1].length); const inner = s.slice(m[1].length + 1, end > 0 ? end : s.length);
      const parts = splitTop(inner, ','); const last = parts[parts.length - 1]; const num = /^\d+$/.test(last) ? Number(last) : null;
      const type = /^hash/i.test(m[1]) ? 'hash' : /^range/i.test(m[1]) ? 'range' : /^round/i.test(m[1]) ? 'roundrobin' : 'keygrouped';
      return { type, keys: type === 'roundrobin' ? [] : (num != null ? parts.slice(0, -1) : parts), num: type === 'roundrobin' ? Number(parts[0]) : num, rest: s.slice((end > 0 ? end : s.length) + 1).replace(/^\s*,\s*/, '') };
    }
    if ((m = /^SinglePartition\b/.exec(s))) return { type: 'single', keys: [], num: 1, rest: s.slice(m[0].length).replace(/^\s*,\s*/, '') };
    if ((m = /^(HashedRelationBroadcastMode|IdentityBroadcastMode|BroadcastMode)/.exec(s))) return { type: 'broadcast', keys: [], num: null, rest: s };
    if ((m = /^UnknownPartitioning\((\d+)\)/.exec(s))) return { type: 'unknown', keys: [], num: Number(m[1]), rest: s.slice(m[0].length).replace(/^\s*,\s*/, '') };
    return null;
  }
  const JOIN_TYPES = /^(Inner|Cross|LeftOuter|RightOuter|FullOuter|LeftSemi|LeftAnti|ExistenceJoin\(.*\)|LeftSingle|UsingJoin.*)$/;
  function parseLocation(v) {
    if (!v) return null; const out = { raw: v };
    let m = /^(\w+)(?:\((\d+) paths?\))?\s*\[(.*)\]?$/s.exec(v.trim());
    if (m) { out.type = m[1]; if (m[2]) out.numPaths = Number(m[2]); out.paths = splitTop(m[3].replace(/\]$/, ''), ','); }
    return out;
  }
  function fillScan(n, rest) {
    const a = n.attrs; let r = rest;
    let m = /^([\w.`$-]+)\s*(?=\[)/.exec(r); if (m && n.name !== 'BatchScan') { a.table = m[1]; r = r.slice(m[0].length); }
    if (n.name === 'BatchScan') { const bm = /^(\S+?)\[/.exec(r); if (bm) { a.table = bm[1]; r = r.slice(bm[1].length); } }
    if (r.startsWith('[')) { const end = matchClose(r, 0); a.output = parseList(r.slice(0, end > 0 ? end + 1 : r.length)); r = end > 0 ? r.slice(end + 1).trim() : ''; }
    if (n.name === 'BatchScan') { const sm = /^([\w.$`-]+(?: \([^)]*\))?(?: \[[^\]]*\])?)\s+(?=[A-Z]\w*:)/.exec(r); if (sm) { a.scanClass = sm[1]; r = r.slice(sm[0].length); } }
    keyValues(r).forEach(([k, v]) => setScanKey(a, k, v));
  }
  function setScanKey(a, k, v) {
    const key = k.replace(/\s+/g, '');
    switch (key) {
      case 'Batched': a.batched = v === 'true'; break;
      case 'DataFilters': a.dataFilters = parseList(v); break;
      case 'Format': a.format = v; break;
      case 'Location': a.location = parseLocation(v); break;
      case 'PartitionFilters': a.partitionFilters = parseList(v); break;
      case 'PushedFilters': a.pushedFilters = parseList(v); break;
      case 'ReadSchema': { a.readSchema = v; const sc = schemaCols(v); a.readCols = sc.cols; a.readSchemaTruncated = sc.truncated; break; }
      case 'RequiredDataFilters': a.requiredDataFilters = parseList(v); break;
      case 'DictionaryFilters': a.dictionaryFilters = parseList(v); break;
      case 'OptionalDataFilters': a.optionalDataFilters = parseList(v); break;
      case 'RuntimeFilters': a.runtimeFilters = parseList(v); break;
      case 'PushedAggregation': a.pushedAggregation = parseList(v); break;
      case 'PushedGroupBy': a.pushedGroupBy = parseList(v); break;
      case 'Output': a.output = parseList(v); break;
      default: (a.extra = a.extra || {})[k] = v;
    }
  }
  function fillJoin(n, rest) {
    const a = n.attrs; const { lists, rest: r } = leadingLists(rest);
    if (n.name !== 'CartesianProduct' && lists.length >= 2) { a.leftKeys = lists[0]; a.rightKeys = lists[1]; }
    const toks = splitTop(r, ',');
    const cond = [];
    toks.forEach((t) => {
      if (JOIN_TYPES.test(t)) a.joinType = t;
      else if (/^Build(Left|Right)$/.test(t)) a.buildSide = t === 'BuildLeft' ? 'left' : 'right';
      else if (/^(true|false)$/.test(t)) a.nullAware = a.nullAware == null ? t === 'true' : a.nullAware;
      else cond.push(t);
    });
    if (n.name === 'CartesianProduct' && lists.length) cond.unshift(lists[0].join(', '));
    if (cond.length) a.condition = cond.join(', ');
    if (n.flags && n.flags.skew != null) a.skew = n.flags.skew;
  }
  function fillAttrs(n, rest) {
    const a = n.attrs; const name = n.name; let m;
    n.args = rest;
    if (n.category === 'scan' && /^(FileScan|Scan|PhotonScan|BatchScan|ColumnarScan|GpuScan)/.test(name)) { fillScan(n, rest); return; }
    if (name === 'Relation') { const rm = /^([\w.`$-]*)\[(.*?)\]\s*(\w+)?/.exec(rest); if (rm) { a.table = rm[1] || null; a.output = splitTop(rm[2], ','); a.format = rm[3] || null; } return; }
    if (/^(Exchange|ShuffleExchange|ColumnarExchange|PhotonShuffleExchangeSink|GpuColumnarExchange)$/.test(name)) {
      const p = parsePartitioning(rest); if (p) { a.partitioning = { type: p.type, keys: p.keys, num: p.num }; const toks = splitTop(p.rest, ','); toks.forEach((t) => { if (/^[A-Z_]{4,}$/.test(t)) a.shuffleOrigin = t; else if ((m = /plan_id=(\d+)/.exec(t))) a.planId = m[1]; }); }
      if ((m = /\[plan_id=(\d+)\]/.exec(rest))) a.planId = m[1];
      return;
    }
    if (n.category === 'broadcast') { a.partitioning = { type: 'broadcast', keys: [], num: null }; a.mode = rest.replace(/,\s*\[plan_id=\d+\]$/, ''); if ((m = /\[plan_id=(\d+)\]/.exec(rest))) a.planId = m[1]; return; }
    if (name === 'ReusedExchange') { const { lists, rest: r } = leadingLists(rest); a.output = lists[0] || []; a.reuses = r; if ((m = /\[plan_id=(\d+)\]/.exec(r))) a.planId = m[1]; else if ((m = /\[id=#(\d+)\]/.exec(r))) a.planId = m[1]; a.reusesBroadcast = /^BroadcastExchange/.test(r); return; }
    if (n.category === 'join') { fillJoin(n, rest); return; }
    if (n.category === 'agg') {
      const na = namedArgs(rest);
      if (na.keys != null) a.keys = parseList(na.keys); if (na.functions != null) a.functions = parseList(na.functions); if (na.output != null) a.output = parseList(na.output);
      return;
    }
    if (/^(Sort|PhotonSort)$/.test(name)) { const { lists, rest: r } = leadingLists(rest); a.sortOrder = lists[0] || []; const t = splitTop(r, ','); if (t[0]) a.global = t[0] === 'true'; return; }
    if (name === 'TakeOrderedAndProject') { const na = namedArgs(rest); a.limit = Number(na.limit); a.sortOrder = parseList(na.orderBy); a.output = parseList(na.output); return; }
    if (/^(Window|PhotonWindow)$/.test(name)) {
      const { lists } = leadingLists(rest); a.windowExprs = lists[0] || [];
      if (lists.length >= 3) { a.partitionSpec = lists[1]; a.orderSpec = lists[2]; }
      else if (lists.length === 2) { if (lists[1].some((x) => /\b(ASC|DESC)\b/.test(x))) { a.partitionSpec = []; a.orderSpec = lists[1]; } else { a.partitionSpec = lists[1]; a.orderSpec = []; } }
      else { a.partitionSpec = []; a.orderSpec = []; }
      return;
    }
    if (/^(Project|PhotonProject)$/.test(name)) { a.output = parseList(rest); return; }
    if (/^(Filter|PhotonFilter)$/.test(name)) { a.condition = rest; return; }
    if (/^(Expand|PhotonExpand)$/.test(name)) { const { lists } = leadingLists(rest); const proj = lists[0] || []; a.projections = proj.length; a.output = lists[1] || []; return; }
    if (/^(Generate|PhotonGenerate)$/.test(name)) { const t = splitTop(rest, ','); a.generator = t[0]; a.outer = t.includes('true'); a.output = parseList(t[t.length - 1]); return; }
    if (n.category === 'udf') { const { lists, rest: r } = leadingLists(rest); a.udfs = lists[0] || []; a.output = lists[1] || []; if (r) a.extraArgs = r; return; }
    if (/^(Coalesce|CollectLimit|GlobalLimit|LocalLimit|PhotonLimit)$/.test(name)) { const num = /^(-?\d+)/.exec(rest); if (num) a.num = Number(num[1]); return; }
    if (name === 'InMemoryTableScan') { const { lists } = leadingLists(rest); a.output = lists[0] || []; a.predicates = lists[1] || []; return; }
    if (name === 'InMemoryRelation') { const { lists, rest: r } = leadingLists(rest); a.output = lists[0] || []; a.storageLevel = r; return; }
    if (/^(AQEShuffleRead|CustomShuffleReader)$/.test(name)) { a.readMode = rest.trim() || 'default'; return; }
    if (/QueryStage$/.test(name)) { const num = /^(\d+)/.exec(rest); if (num) a.stageId = Number(num[1]); return; }
    if (name === 'AdaptiveSparkPlan') { a.isFinalPlan = /isFinalPlan=true/.test(rest); return; }
    if (n.category === 'subquery') { a.subqueryId = (/(subquery#\d+|dynamicpruning#\d+)/.exec(rest) || [])[1] || null; return; }
    if (n.category === 'write') { a.command = name; const t = splitTop(rest, ','); a.target = t[0] || ''; return; }
  }

  let seq = 0;
  function makeNode(text, line, formatted) {
    const node = { id: 'n' + (++seq), name: '', text: '', args: '', codegen: null, children: [], line, attrs: {}, details: [], category: 'other', subquery: false, opId: null, stats: null, metrics: null };
    let t = text;
    if (formatted) {
      const fm = FORMATTED_LINE.exec(t);
      if (fm) { node.codegen = fm[1] ? 0 : null; t = fm[2]; node.opId = Number(fm[3]); if (fm[4]) node.stats = splitStats(fm[4]).stats; }
    } else {
      const cm = /^\*\((\d+)\)\s+/.exec(t); if (cm) { node.codegen = Number(cm[1]); t = t.slice(cm[0].length); }
      const st = splitStats(t); if (st.stats) { node.stats = st.stats; t = st.text; }
    }
    node.text = t;
    const nm = nameOf(t); node.name = nm.name; node.flags = nm.flags || {}; node.unresolved = !!nm.unresolved;
    node.category = categoryOf(node.name);
    node.subquery = node.category === 'subquery';
    if (formatted) { node.headerArgs = nm.rest; if (node.category === 'scan' && nm.format) node.attrs.format = nm.format; const tm = /^([\w.`$-]+)$/.exec(nm.rest || ''); if (tm && node.category === 'scan') node.attrs.table = tm[1]; if (node.category === 'join') fillJoin(node, nm.rest.split(/\s+/).join(', ')); if (/QueryStage$/.test(node.name) || node.name === 'AQEShuffleRead') {} }
    else fillAttrs(node, nm.rest);
    if (nm.format && !node.attrs.format) node.attrs.format = nm.format;
    return node;
  }

  // ---------- tree parsing ----------
  // Lines of one tree (no blank lines). Returns roots. Nesting follows the column where each node's text starts.
  function parseTree(lines, firstLine, formatted, notices) {
    const roots = []; const stack = [];
    lines.forEach((raw, i) => {
      const gm = /^([ :|]*?)([+:]- )(.*)$/.exec(raw);
      let col, text, glyph = !!gm;
      if (gm) { col = gm[1].length + 3; text = gm[3]; } else { const m = /^( *)(.*)$/.exec(raw); col = m[1].length; text = m[2]; }
      if (!text.trim()) return;
      const node = makeNode(text, firstLine + i, formatted);
      node.col = col;
      // A line directly under a "== Final Plan ==" marker is printed at the marker's own column without a glyph.
      const top = stack[stack.length - 1];
      if (!glyph && top && top.node.category === 'marker' && col === top.col) { top.node.children.push(node); stack.push({ col: col + 0.5, node }); return; }
      while (stack.length && stack[stack.length - 1].col >= col) stack.pop();
      if (!stack.length) {
        if (roots.length && notices) notices.push({ level: 'info', text: 'Line ' + (firstLine + i + 1) + ' starts a second plan tree; both are shown.' });
        roots.push(node);
      } else stack[stack.length - 1].node.children.push(node);
      stack.push({ col, node });
    });
    return roots;
  }
  // Formatted mode: "(n) Name [codegen id : k]" blocks of "Key [count]: value" lines.
  function parseDetails(lines, startIdx, details) {
    let i = startIdx; let cur = null;
    for (; i < lines.length; i++) {
      const l = lines[i];
      if (/^===== .* =====$/.test(l) || /^== .* ==$/.test(l) || /^Subquery:\d+ /.test(l) || /^Subplan:\d+/.test(l)) break;
      const h = /^\((\d+)\) (.*?)\s*(?:\[codegen id : (\d+)\])?\s*$/.exec(l);
      if (h) { cur = { id: Number(h[1]), header: h[2], codegen: h[3] ? Number(h[3]) : null, kv: [] }; details[cur.id] = cur; continue; }
      if (!l.trim()) { cur = null; continue; }
      if (cur) { const kv = /^([A-Za-z][\w ]*?)(?: \[(\d+)\])?\s?: ?(.*)$/.exec(l); if (kv) cur.kv.push([kv[1].trim(), kv[3]]); else if (cur.kv.length) cur.kv[cur.kv.length - 1][1] += '\n' + l; }
    }
    return i;
  }
  function applyDetails(node, d) {
    if (!d) return;
    node.details = d.kv.slice(); if (d.codegen != null) node.codegen = d.codegen; else if (node.codegen === 0) node.codegen = null;
    const a = node.attrs; const get = (k) => { const e = d.kv.find(([kk]) => kk === k); return e ? e[1] : null; };
    d.kv.forEach(([k, v]) => { if (node.category === 'scan' && /^(Output|Batched|Location|PartitionFilters|PushedFilters|DataFilters|ReadSchema|RequiredDataFilters|DictionaryFilters|RuntimeFilters|PushedAggregation|PushedGroupBy)$/.test(k)) setScanKey(a, k, v); });
    if (node.category === 'scan') {
      if (a.location && a.location.raw) { const lm = /^(\w+)\s*\[(.*)\]$/.exec(a.location.raw); if (lm) { a.location = { raw: a.location.raw, type: lm[1], paths: splitTop(lm[2], ','), numPaths: splitTop(lm[2], ',').length }; } }
      if (a.partitionFilters == null && a.readSchema != null) a.partitionFilters = [];
      if (a.pushedFilters == null && a.readSchema != null) a.pushedFilters = [];
      if (a.dataFilters == null) a.dataFilters = null; // formatted mode doesn't print DataFilters
    }
    const out = get('Output'); if (out != null && !a.output) a.output = parseList(out);
    const inp = get('Input'); if (inp != null) a.input = parseList(inp);
    const args = get('Arguments');
    if (node.category === 'exchange' && args) fillAttrs(node, args);
    else if (node.category === 'broadcast' && args) fillAttrs(node, args);
    else if (/^(Sort|PhotonSort)$/.test(node.name) && args) fillAttrs(node, args);
    else if (node.name === 'AdaptiveSparkPlan' && args) a.isFinalPlan = /isFinalPlan=true/.test(args);
    else if ((node.name === 'AQEShuffleRead') && args) a.readMode = args.trim();
    else if (/QueryStage$/.test(node.name) && args) a.stageId = Number(args);
    else if (/^(Coalesce|CollectLimit|GlobalLimit|LocalLimit)$/.test(node.name) && args) { const num = /(-?\d+)/.exec(args); if (num) a.num = Number(num[1]); }
    else if (node.category === 'udf' && args) a.extraArgs = args;
    else if (node.name === 'ReusedExchange' && args) { a.reuses = args; }
    else if (node.name === 'TakeOrderedAndProject' && args) { const t = splitTop(args, ','); a.limit = Number(t[0]); }
    if (node.category === 'agg') { const k = get('Keys'); if (k != null) a.keys = parseList(k); const f = get('Functions'); if (f != null) a.functions = parseList(f); const r = get('Results'); if (r != null) a.output = parseList(r); }
    if (node.category === 'join') { const lk = get('Left keys'); if (lk != null) a.leftKeys = parseList(lk); const rk = get('Right keys'); if (rk != null) a.rightKeys = parseList(rk); const jt = get('Join type'); if (jt) a.joinType = jt; const jc = get('Join condition'); if (jc && jc !== 'None') a.condition = jc; }
    if (/^(Filter|PhotonFilter)$/.test(node.name)) { const c = get('Condition'); if (c) a.condition = c; }
    if (/^(Window)$/.test(node.name)) { const ar = get('Arguments'); if (ar) { const { lists } = leadingLists(ar); a.windowExprs = lists[0] || []; a.partitionSpec = lists[1] || []; a.orderSpec = lists[2] || []; } }
    if (/^(Expand)$/.test(node.name)) { const ar = get('Arguments'); if (ar) { const { lists } = leadingLists(ar); a.projections = (lists[0] || []).length; } }
    if (/^(Generate)$/.test(node.name)) { const ar = get('Arguments'); if (ar) a.generator = splitTop(ar, ',')[0]; }
    if (node.category === 'udf') { const f = get('Functions') || get('Arguments'); if (f) a.udfs = parseList(f); }
  }

  function walk(rootNode, fn, depth, parent) {
    if (!rootNode) return; fn(rootNode, depth || 0, parent || null);
    rootNode.children.forEach((c) => walk(c, fn, (depth || 0) + 1, rootNode));
  }
  // Remove the AQE markers: AdaptiveSparkPlan's children become the final plan (or the initial one if not final).
  function resolveAqe(rootNode) {
    const out = { root: rootNode, initial: null, final: null };
    if (!rootNode) return out;
    let asp = null;
    walk(rootNode, (n) => { if (!asp && n.name === 'AdaptiveSparkPlan' && n.children.some((c) => c.category === 'marker')) asp = n; });
    // Also fix nested AQE plans (subqueries) in place.
    walk(rootNode, (n) => {
      const markers = n.children.filter((c) => c.category === 'marker');
      if (!markers.length) return;
      const fin = markers.find((c) => /Final Plan/i.test(c.name)); const ini = markers.find((c) => /Initial Plan/i.test(c.name));
      const keep = n.children.filter((c) => c.category !== 'marker');
      n.children = keep.concat(fin ? fin.children : ini ? ini.children : []);
      if (n === asp) { out.final = fin ? n : null; out.initial = ini ? ini.children : null; if (fin && ini) n.initialChildren = ini.children; }
      else if (fin && ini) n.initialChildren = ini.children;
    });
    return out;
  }
  function detectVersion(text) {
    if (/Photon[A-Z]/.test(text)) return 'Databricks (Photon)';
    if (/ResultQueryStage/.test(text)) return 'Spark 4.x';
    if (/\[plan_id=\d+\]/.test(text)) return 'Spark 3.4+';
    if (/AQEShuffleRead/.test(text)) return 'Spark 3.2+';
    if (/CustomShuffleReader/.test(text)) return 'Spark 3.0–3.1';
    if (/\[id=#\d+\]/.test(text)) return 'Spark 3.x';
    return null;
  }
  function isPhysicalText(lines) {
    return lines.some((l) => /(Exchange|FileScan|Scan \w+|HashAggregate|Project \[|BroadcastHashJoin|SortMergeJoin|AdaptiveSparkPlan|\*\(\d+\)|LocalTableScan|Photon[A-Z]|Execute \w)/.test(l));
  }

  // ---------- main entry ----------
  function parsePlan(text, opts) {
    opts = opts || {}; seq = opts.idStart || 0;
    const notices = []; const lines = normalize(text);
    const res = { input: 'plan', format: 'simple', sections: [], physical: null, initialPhysical: null, logical: {}, subqueries: [], photon: null, notices, version: detectVersion(String(text || '')), aqe: { present: false, final: false } };
    if (!lines.some((l) => l.trim())) { notices.push({ level: 'error', text: 'Nothing to read. Paste the output of EXPLAIN or df.explain().' }); return finish(res); }
    // Split into sections by "== Title ==" headers at column 0.
    const secs = []; let cur = null; let codegenFound = false;
    lines.forEach((l, i) => {
      const h = /^==\s*(.+?)\s*==\s*$/.exec(l);
      if (h && !/^===/.test(l) && !/^==\s*(Final|Initial) Plan/i.test(l)) { cur = { title: h[1], start: i + 1, lines: [] }; secs.push(cur); return; }
      if (/^Found \d+ WholeStageCodegen subtrees?\.?/.test(l)) { codegenFound = true; return; }
      if (!cur) { cur = { title: null, start: i, lines: [] }; secs.push(cur); }
      cur.lines.push(l);
    });
    const titled = secs.filter((s) => s.title);
    if (codegenFound || titled.some((s) => /^Subtree \d+/.test(s.title))) res.format = 'codegen';
    else if (titled.some((s) => /Parsed Logical Plan/i.test(s.title))) res.format = 'extended';
    else if (titled.some((s) => /Optimized Logical Plan/i.test(s.title)) && lines.some((l) => /Statistics\(sizeInBytes/.test(l))) res.format = 'cost';
    secs.forEach((s) => {
      const kind = !s.title ? (isPhysicalText(s.lines) ? 'physical' : (s.lines.some((l) => /^[ :+-]*'?(Aggregate|Join|Relation|Project|Filter|SubqueryAlias|LogicalRDD|Window|Sort)\b/.test(l)) ? 'logical' : 'other'))
        : /Physical Plan/i.test(s.title) ? 'physical' : /Logical Plan/i.test(s.title) ? 'logical' : /^Subtree \d+/.test(s.title) ? 'codegen' : /Photon Explanation/i.test(s.title) ? 'photon' : 'other';
      res.sections.push({ title: s.title || (kind === 'physical' ? 'Physical Plan' : kind === 'logical' ? 'Logical Plan' : 'Text'), kind, text: s.lines.join('\n').replace(/\n+$/, '') });
      s.kind = kind;
    });
    // Physical plan
    const phys = secs.filter((s) => s.kind === 'physical');
    if (phys.length > 1) notices.push({ level: 'info', text: phys.length + ' physical plans found; showing the first. Use Compare to diff two plans.' });
    if (phys.length) readPhysical(phys[0], res, notices);
    else if (res.format === 'codegen') {
      const subs = secs.filter((s) => s.kind === 'codegen');
      const roots = [];
      subs.forEach((s) => { const tl = []; for (const l of s.lines) { if (!l.trim() || /^Generated code:/.test(l)) break; tl.push(l); } roots.push(...parseTree(tl, s.start, false, null)); });
      const top = makeNode('Codegen subtrees (' + roots.length + ')', 0, false); top.name = 'WholeStageCodegen subtrees'; top.category = 'stage'; top.children = roots;
      res.physical = top;
      notices.push({ level: 'info', text: 'Codegen output lists each whole-stage-codegen subtree on its own, so the subtrees are shown side by side and the generated Java code is skipped. For the full plan use EXPLAIN FORMATTED or plain EXPLAIN.' });
    }
    // Logical plans
    secs.filter((s) => s.kind === 'logical').forEach((s) => {
      const tl = s.lines.filter((l) => l.trim());
      const body = /^[\w]+: /.test(tl[0] || '') && !/^[+:]- /.test(tl[0]) && !/[[(]/.test((tl[0] || '').split(':')[0]) ? tl.slice(1) : tl; // Analyzed plan starts with the output schema
      const key = !s.title ? 'logical' : /Parsed/i.test(s.title) ? 'parsed' : /Analyzed/i.test(s.title) ? 'analyzed' : 'optimized';
      try { const roots = parseTree(body, s.start, false, null); res.logical[key] = roots[0] || null; } catch (e) { notices.push({ level: 'warn', text: 'Could not read the ' + key + ' logical plan.' }); }
    });
    const ph = secs.find((s) => s.kind === 'photon'); if (ph) res.photon = { explanation: ph.lines.join('\n').trim() };
    if (!res.physical) {
      if (Object.keys(res.logical).length) notices.push({ level: 'warn', text: 'Only a logical plan was found. Most diagnostics need the physical plan, so paste the output of EXPLAIN (or EXPLAIN EXTENDED / FORMATTED) for the full picture.' });
      else notices.push({ level: 'error', text: 'No Spark plan found in this text. Paste the output of EXPLAIN, df.explain(), or a Spark event log.' });
    }
    if (/^(Execute \w+)/.test(res.physical ? res.physical.name : '') && res.physical.children.some((c) => /SubqueryAlias|Relation|^View$/.test(c.name) || c.children.some((g) => /SubqueryAlias|^Relation$|^View$|^Project$/.test(g.name) && !g.codegen))) {
      notices.push({ level: 'info', text: 'This is a command plan (for example CREATE TABLE AS SELECT); Spark plans the inner query when the command runs. To analyze the query itself, EXPLAIN the SELECT on its own.' });
    }
    return finish(res);
  }
  function readPhysical(sec, res, notices) {
    const ls = sec.lines; let i = 0; while (i < ls.length && !ls[i].trim()) i++;
    const treeLines = []; for (; i < ls.length && ls[i].trim(); i++) treeLines.push(ls[i]);
    const formatted = treeLines.length > 0 && treeLines.every((l) => FORMATTED_LINE.test(l.replace(/^[ :|]*([+:]- )?/, '').trim()) || /==\s*(Final|Initial) Plan\s*==/.test(l)) && ls.slice(i).some((l) => /^\(\d+\) /.test(l));
    if (formatted) res.format = res.format === 'simple' ? 'formatted' : res.format;
    let roots;
    try { roots = parseTree(treeLines, sec.start + (i - treeLines.length), formatted, notices); } catch (e) { notices.push({ level: 'error', text: 'The plan tree could not be read: ' + e.message }); roots = []; }
    if (!roots.length) return;
    if (formatted) {
      const details = {}; let j = parseDetails(ls, i, details);
      const subq = [];
      while (j < ls.length) {
        const l = ls[j];
        const sm = /^Subquery:(\d+) Hosting operator id = (\d+) Hosting Expression = (.*)$/.exec(l);
        if (sm) {
          const tl = []; let k = j + 1; for (; k < ls.length && ls[k].trim(); k++) tl.push(ls[k]);
          const sr = parseTree(tl, sec.start + j + 1, true, null);
          if (/^Subquery:\d+ Hosting operator id = \d+ Hosting Expression = ReusedSubquery/.test(l) && !sr.length) { j = k; continue; }
          subq.push({ host: Number(sm[2]), expr: sm[3], roots: sr });
          j = parseDetails(ls, k, details); continue;
        }
        j++;
      }
      const all = [];
      roots.forEach((r) => walk(r, (n) => all.push(n)));
      subq.forEach((s) => s.roots.forEach((r) => walk(r, (n) => all.push(n))));
      all.forEach((n) => applyDetails(n, details[n.opId]));
      subq.forEach((s) => {
        const host = all.find((n) => n.opId === s.host && !n._isSub);
        s.roots.forEach((r) => { const wrap = makeNode('Subquery ' + s.expr.replace(/^Subquery\s*/, ''), 0, false); wrap.name = /^ReusedSubquery/.test(s.expr) ? 'ReusedSubquery' : /dynamicpruning/.test(s.expr) ? 'SubqueryBroadcast' : 'Subquery'; wrap.category = 'subquery'; wrap.subquery = true; wrap.children = [r]; walk(r, (n) => { n._isSub = true; }); if (host) host.children.unshift(wrap); else res.subqueries.push(wrap); });
      });
    }
    let main = roots[0];
    if (roots.length > 1) { const top = makeNode('Plans (' + roots.length + ')', 0, false); top.name = 'Plans'; top.category = 'other'; top.children = roots; main = top; }
    const aq = resolveAqe(main);
    res.aqe.present = false; walk(main, (n) => { if (n.name === 'AdaptiveSparkPlan') { res.aqe.present = true; if (n.attrs.isFinalPlan) res.aqe.final = true; } });
    if (aq.initial) { const ini = makeNode('AdaptiveSparkPlan isFinalPlan=false', 0, false); ini.children = aq.initial; res.initialPhysical = ini; }
    res.physical = main;
  }
  function finish(res) {
    // Flat node list, depth, parents, stages and reuse links.
    res.nodes = [];
    const visit = (r, sub) => walk(r, (n, d, p) => { n.depth = d; n.parentId = p ? p.id : null; if (sub || (p && p.subqueryTree) || n.subquery) n.subqueryTree = true; res.nodes.push(n); });
    if (res.physical) visit(res.physical, false);
    res.subqueries.forEach((s) => visit(s, true));
    if (res.physical) assignStages(res.physical);
    linkReuse(res.nodes);
    res.nodes.forEach((n) => { delete n.col; delete n._isSub; });
    if (res.initialPhysical) { walk(res.initialPhysical, (n, d, p) => { n.depth = d; n.parentId = p ? p.id : null; delete n.col; }); assignStages(res.initialPhysical); }
    res.ok = !!res.physical;
    return res;
  }
  // A stage boundary is every exchange: the exchange writes its child's data, the parent reads it.
  function assignStages(r) {
    let next = 0;
    const go = (n, stage) => {
      n.stage = stage;
      n.children.forEach((c) => {
        if (isExchange(c) || c.category === 'subquery') { const s = ++next; go(c, s); }
        else go(c, stage);
      });
    };
    go(r, 0);
  }
  function linkReuse(nodes) {
    const byPlan = {}; nodes.forEach((n) => { if (n.attrs.planId && n.name !== 'ReusedExchange') byPlan[n.attrs.planId] = n; });
    nodes.forEach((n) => { if (n.name === 'ReusedExchange' && n.attrs.planId && byPlan[n.attrs.planId]) n.reuseOf = byPlan[n.attrs.planId].id; });
  }
  // Helpers used by diagnostics and views
  const scanParts = (n) => {
    const a = n.attrs; const out = (a.output || []).map(attrName); const read = a.readCols || [];
    const partCols = read.length || a.readSchema != null ? out.filter((c) => !read.includes(c) && !(a.readSchemaTruncated)) : [];
    return { out, read, partCols };
  };
  function shortLabel(n) {
    const a = n.attrs;
    if (n.category === 'scan') { const loc = a.location && a.location.paths && a.location.paths[0]; const tbl = a.table || (loc ? loc.replace(/\/+$/, '').split('/').pop() : ''); return tbl; }
    if (n.category === 'exchange' && a.partitioning) { const p = a.partitioning; return p.type === 'single' ? 'single partition' : p.type + (p.keys && p.keys.length ? '(' + p.keys.map(attrName).join(', ') + ')' : '') + (p.num != null ? ' → ' + p.num : ''); }
    if (n.category === 'join') return [a.joinType, a.buildSide ? 'build ' + a.buildSide : '', a.leftKeys ? a.leftKeys.map(attrName).join(', ') : ''].filter(Boolean).join(' · ');
    if (n.category === 'agg' && a.keys) return a.keys.length ? 'by ' + a.keys.map(attrName).join(', ') : 'global';
    if (n.name === 'ReusedExchange') return 'reuses an earlier exchange';
    if (n.category === 'udf') return (a.udfs || []).map((u) => u.replace(/#\d+L?/g, '')).join(', ');
    if (n.category === 'window') return a.partitionSpec && a.partitionSpec.length ? 'partition by ' + a.partitionSpec.map(attrName).join(', ') : 'no partitionBy';
    if (n.category === 'limit' && a.num != null) return String(a.num);
    if (a.readMode) return a.readMode;
    if (a.stageId != null) return 'stage ' + a.stageId;
    return '';
  }
  return { parsePlan, finish, assignStages, normalize, parseTree, makeNode, categoryOf, isExchange, isShuffle, isPhoton, walk, scanParts, shortLabel, parsePartitioning, detectVersion, fillAttrs, resetSeq: (v) => { seq = v || 0; }, nextId: () => 'n' + (++seq) };
});
