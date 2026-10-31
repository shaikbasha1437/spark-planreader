/* Spark PlanReader diagnostics: rules over a parsed plan. Each finding says what was seen, why it matters
   and how to fix it (PySpark, a SQL hint and a config line). Rules only use what the plan or event log
   actually shows; when a size is unknown the finding says so instead of guessing. */
(function (root, factory) {
  const m = factory(root.SP ? root.SP.parser : require('./parser.js'), root.SP ? root.SP.text : require('./textutil.js'));
  if (typeof module === 'object' && module.exports) module.exports = m;
  else (root.SP = root.SP || {}).diagnose = m;
})(typeof self !== 'undefined' ? self : this, function (P, T) {
  'use strict';
  const { attrName, fmtBytes, fmtNum } = T;
  const SEV_ORDER = { high: 0, medium: 1, low: 2, info: 3 };
  const DEFAULT_BROADCAST = 10 * 1024 * 1024;

  // ---------- helpers ----------
  const walkSub = (n, fn) => P.walk(n, fn);
  function firstScan(n) { let s = null; walkSub(n, (x) => { if (!s && (x.category === 'scan' || x.category === 'cache')) s = x; }); return s; }
  let BYID = {};
  function tableOf(n) {
    let s = firstScan(n);
    if (!s) { let re = null; P.walk(n, (x) => { if (!re && x.reuseOf && BYID[x.reuseOf]) re = BYID[x.reuseOf]; }); if (re) s = firstScan(re); }
    if (!s) return null;
    if (s.category === 'cache') return 'cached_df';
    if (s.name === 'Range') { const m = /\((-?\d+),\s*(-?\d+)/.exec(s.args || ''); return m ? 'spark.range(' + m[1] + ', ' + m[2] + ')' : 'spark.range'; }
    if (s.name === 'LocalTableScan') return 'local rows';
    const a = s.attrs; let t = a.table || (a.location && a.location.paths && a.location.paths[0] ? a.location.paths[0].replace(/\/+$/, '').split('/').pop() : null);
    if (t) t = t.replace(/^spark_catalog\.\w+\./, '').replace(/[`]/g, '').split('.').pop();
    return t || null;
  }
  function metric(n, ...names) { if (!n.metrics) return null; for (const k of names) if (n.metrics[k]) return n.metrics[k]; return null; }
  // Best known size of what a join side produces: query-stage statistics, cost-mode statistics, or event-log metrics.
  function sideSize(n, res) {
    let found = null;
    const go = (x) => {
      if (found) return;
      if (x.stats && x.stats.sizeInBytes != null) { found = { bytes: x.stats.sizeInBytes, rows: x.stats.rowCount, source: 'plan statistics' }; return; }
      const ds = (P.isExchange(x) || x.category === 'broadcast') && metric(x, 'data size'); if (ds) { found = { bytes: ds.value, rows: (metric(x, 'records read', 'number of output rows') || {}).value, source: 'event log metrics' }; return; }
      if (x.category === 'scan') { const sz = metric(x, 'size of files read'); if (sz) { found = { bytes: sz.value, rows: (metric(x, 'number of output rows') || {}).value, source: 'event log metrics (files read)' }; return; } }
      x.children.forEach(go);
    };
    go(n);
    if (!found && n._costSize != null) found = { bytes: n._costSize, source: 'cost-based statistics' };
    return found;
  }
  // EXPLAIN COST: logical Join nodes carry Statistics(sizeInBytes); map them to physical joins in order.
  function attachCostSizes(res) {
    const lg = res.logical && (res.logical.optimized || res.logical.logical); if (!lg) return;
    const ljoins = []; P.walk(lg, (n) => { if (n.name === 'Join') ljoins.push(n); });
    const pjoins = res.nodes.filter((n) => n.category === 'join' && !n.subqueryTree);
    if (!ljoins.length || ljoins.length !== pjoins.length) return;
    pjoins.forEach((pj, i) => { const lj = ljoins[i]; pj.children.slice(-2).forEach((c, k) => { const lc = lj.children[k]; if (lc && lc.stats && lc.stats.sizeInBytes != null) c._costSize = lc.stats.sizeInBytes; }); });
  }
  function conf(res, key) { return res.conf && res.conf[key] != null ? String(res.conf[key]) : null; }
  function cols(list) { return (list || []).map(attrName).filter(Boolean); }
  function findingsSort(a, b) { return SEV_ORDER[a.severity] - SEV_ORDER[b.severity] || a.order - b.order; }
  const q = (s) => s.replace(/'/g, "\\'");

  // ---------- rules ----------
  const RULES = [];
  const rule = (id, title, fn) => RULES.push({ id, title, fn });

  rule('shuffles', 'Shuffles', (ctx) => {
    const sh = ctx.main.filter((n) => P.isShuffle(n) && n.name !== 'ReusedExchange');
    if (!sh.length) return;
    const byOrigin = {}; sh.forEach((n) => { const o = n.attrs.shuffleOrigin || 'ENSURE_REQUIREMENTS'; byOrigin[o] = (byOrigin[o] || 0) + 1; });
    const parts = [...new Set(sh.map((n) => n.attrs.partitioning && n.attrs.partitioning.num).filter((x) => x != null))];
    const sev = sh.length >= 5 ? 'medium' : 'info';
    const user = (byOrigin.REPARTITION_BY_COL || 0) + (byOrigin.REPARTITION_BY_NUM || 0);
    return { severity: sev, title: sh.length + ' shuffle' + (sh.length > 1 ? 's' : '') + ' (' + (sh.length + 1) + ' stages or more)', nodes: sh.map((n) => n.id),
      explain: 'Each Exchange writes all its input to local disk and every task of the next stage fetches its share over the network.' + (user ? ' ' + user + ' of them come from an explicit repartition() call.' : '') + (parts.length ? ' Partition count' + (parts.length > 1 ? 's' : '') + ': ' + parts.join(', ') + '.' : ''),
      why: 'Shuffles are usually the biggest cost in a Spark job: disk writes, network transfer and a full stage barrier. Joins on the same key, groupBy after a join on the same key and broadcast joins can remove some.',
      fix: { pyspark: '# Join small tables with a broadcast instead of a shuffle\nfrom pyspark.sql import functions as F\nresult = big_df.join(F.broadcast(small_df), "key")\n\n# Avoid repartition() right before a groupBy / join on the same key: Spark already shuffles there', sql: 'SELECT /*+ BROADCAST(small) */ ...\nFROM big JOIN small ON big.key = small.key', config: 'spark.sql.adaptive.enabled=true\nspark.sql.adaptive.coalescePartitions.enabled=true' } };
  });

  rule('smj-broadcast', 'Sort-merge join that could be a broadcast', (ctx) => {
    const out = [];
    const threshold = Number(conf(ctx.res, 'spark.sql.autoBroadcastJoinThreshold')) || DEFAULT_BROADCAST;
    ctx.main.filter((n) => n.name === 'SortMergeJoin' || n.name === 'ShuffledHashJoin').forEach((n) => {
      const [l, r] = n.children.slice(-2); if (!l || !r) return;
      const jt = n.attrs.joinType || 'Inner';
      const sl = sideSize(l, ctx.res), sr = sideSize(r, ctx.res);
      // which side can be broadcast for this join type
      const canRight = !/^(RightOuter|FullOuter)$/.test(jt), canLeft = !/^(LeftOuter|FullOuter|LeftSemi|LeftAnti|ExistenceJoin)/.test(jt);
      let small = null, side = null;
      if (sr && canRight && (!sl || sr.bytes <= sl.bytes)) { small = sr; side = r; }
      else if (sl && canLeft) { small = sl; side = l; }
      const tblL = tableOf(l), tblR = tableOf(r);
      const smallName = side ? tableOf(side) || 'small_df' : (tblR || 'small_df');
      const bigName = side === l ? (tblR || 'big_df') : (tblL || 'big_df');
      const fix = { pyspark: 'from pyspark.sql import functions as F\nresult = ' + bigName + '.join(F.broadcast(' + smallName + '), ' + (n.attrs.leftKeys ? JSON.stringify(cols(n.attrs.leftKeys)[0] || 'key') : '"key"') + (jt !== 'Inner' ? ', "' + jt.replace(/Outer$/, '_outer').toLowerCase().replace('leftsemi', 'left_semi').replace('leftanti', 'left_anti') + '"' : '') + ')', sql: 'SELECT /*+ BROADCAST(' + smallName + ') */ ...', config: '# raise the automatic broadcast limit (default 10MB); keep it well under executor memory\nspark.sql.autoBroadcastJoinThreshold=' + (small && small.bytes > threshold ? Math.ceil(small.bytes * 1.5 / 1048576) + 'MB' : '100MB') + '\nspark.sql.adaptive.enabled=true' };
      const sizes = 'Left: ' + (sl ? fmtBytes(sl.bytes) : 'unknown size') + ' (' + (tblL || '?') + '), right: ' + (sr ? fmtBytes(sr.bytes) : 'unknown size') + ' (' + (tblR || '?') + ').';
      if (small && small.bytes <= 512 * 1048576) {
        out.push({ severity: small.bytes <= 100 * 1048576 ? 'high' : 'medium', title: n.name + ' where one side is about ' + fmtBytes(small.bytes) + ' — a broadcast join would skip ' + (n.name === 'SortMergeJoin' ? 'two shuffles and two sorts' : 'a shuffle'), nodes: [n.id, side.id], est: { bytes: small.bytes, source: small.source },
          explain: 'Both sides are shuffled' + (n.name === 'SortMergeJoin' ? ' and sorted' : '') + ' by the join key. ' + sizes + ' The ' + (side === r ? 'right' : 'left') + ' side (' + smallName + ', ' + small.source + ') is small enough to broadcast' + (threshold < 0 ? ', but spark.sql.autoBroadcastJoinThreshold is -1 (automatic broadcast turned off), so Spark did not do it automatically.' : small.bytes > threshold ? ', but it is above spark.sql.autoBroadcastJoinThreshold (' + fmtBytes(threshold) + '), so Spark did not do it automatically.' : '. Spark may have picked a sort-merge join because the size was not known when planning (AQE off, or statistics missing).'),
          why: 'A broadcast join sends the small side to every executor once and streams the big side without shuffling or sorting it. Typical speed-ups are several times on the join stage.', fix });
      } else if (!sl && !sr) {
        out.push({ severity: ctx.res.aqe.present ? 'low' : 'medium', title: n.name + ' on ' + (tblL || 'left') + ' × ' + (tblR || 'right') + ' (sizes unknown)', nodes: [n.id],
          explain: 'Both sides are shuffled' + (n.name === 'SortMergeJoin' ? ' and sorted' : '') + ' by ' + (cols(n.attrs.leftKeys).join(', ') || 'the join key') + '. The plan has no size information, so it is not clear whether one side is small. ' + (ctx.res.aqe.present ? 'With AQE on, Spark switches to a broadcast at runtime when one side turns out small; check the final plan (after the query runs) or the event log.' : 'AQE is off, so Spark decided before seeing real sizes.'),
          why: 'If either side is under a few hundred MB, broadcasting it removes two shuffles and two sorts.', fix: Object.assign({}, fix, { notes: 'To see sizes, use EXPLAIN COST, df.explain("cost"), or upload the event log of a run.' }) });
      }
    });
    return out;
  });

  rule('nested-loop', 'Nested loop join or Cartesian product', (ctx) => ctx.main.filter((n) => n.name === 'BroadcastNestedLoopJoin' || n.name === 'CartesianProduct').map((n) => {
    const [l, r] = n.children.slice(-2); const bnl = n.name === 'BroadcastNestedLoopJoin';
    const cond = n.attrs.condition || '';
    const tinyRows = (x) => { let rows = null; P.walk(x, (y) => { if (rows != null) return; if (y.name === 'Range') { const m = /\((-?\d+),\s*(-?\d+)(?:,\s*step=(\d+))?/.exec(y.args || ''); if (m) rows = (Number(m[2]) - Number(m[1])) / (Number(m[3]) || 1); } else if (y.name === 'LocalTableScan' && y.attrs.output) rows = -1; }); return rows; };
    const tr = [l, r].map((x) => (x ? tinyRows(x) : null)).find((v) => v != null && v <= 1000);
    if (tr != null && !cond) return { severity: 'low', title: 'Small cross join with ' + (tr > 0 ? tr + ' generated rows' : 'a local list') + ' (' + (tableOf(l) || 'left') + ' × ' + (tableOf(r) || 'right') + ')', nodes: [n.id],
      explain: 'One side is a tiny generated table (spark.range or a literal list), so the cross join only multiplies the other side by ' + (tr > 0 ? tr : 'a few') + ' rows. This is the usual way to build salt buckets or a date grid.',
      why: 'Fine as long as the other side is small too (here it is copied ' + (tr > 0 ? tr : 'n') + ' times). Keep the generated side small.', fix: null };
    return { severity: 'high', title: (bnl ? 'Broadcast nested loop join' : 'Cartesian product') + ' between ' + (tableOf(l) || 'left') + ' and ' + (tableOf(r) || 'right'), nodes: [n.id],
      explain: (bnl ? 'Every row of the streamed side is compared with every row of the broadcast side' : 'Every row is paired with every row of the other side') + (cond ? ', then filtered by: ' + T.stripIds(cond).slice(0, 200) + '.' : ' with no condition at all.') + ' This happens when the join condition has no plain equality between the two sides (for example only <, >, OR, a function on both sides, or a forgotten ON clause).',
      why: 'Work grows as rows(left) × rows(right): 1M × 1M rows is a trillion comparisons. These joins are the most common cause of jobs that never finish.',
      fix: { pyspark: '# Give the join an equality key; keep the range / OR part as an extra filter\nresult = a.join(b, (a.key == b.key) & (a.ts.between(b.start, b.end)))\n\n# OR conditions: split into two equi-joins and union them\nr1 = a.join(b, a.k1 == b.k1)\nr2 = a.join(b, (a.k2 == b.k2) & (a.k1 != b.k1))\nresult = r1.unionByName(r2)', sql: '-- add an equality to the ON clause\nSELECT ... FROM a JOIN b ON a.key = b.key AND a.ts BETWEEN b.start AND b.end', config: '# fail fast on accidental cross joins (Spark 2 behaviour)\nspark.sql.crossJoin.enabled=false' } };
  }));

  rule('skew', 'Data skew', (ctx) => {
    const out = [];
    ctx.main.forEach((n) => {
      const m = P.isExchange(n) ? metric(n, 'records read') : (n.category === 'join' ? metric(n, 'number of output rows') : null);
      if (!m || !m.tasks || m.tasks.count === 1 || !m.tasks.median && m.tasks.max < 10000) return;
      const ratio = m.tasks.median ? m.tasks.max / m.tasks.median : Infinity;
      if (ratio < 4 || m.tasks.max < 10000) return;
      const key = n.attrs.partitioning ? cols(n.attrs.partitioning.keys).join(', ') : cols(n.attrs.leftKeys).join(', ');
      if (P.isExchange(n) && out.some((f) => f._key === key)) return;
      out.push({ _key: key, severity: ratio >= 8 ? 'high' : 'medium', title: 'Skewed ' + (P.isExchange(n) ? 'shuffle' : 'join') + (key ? ' on ' + key : '') + ': the largest task has ' + (ratio === Infinity ? 'all the' : Math.round(ratio) + '× the median') + ' rows', nodes: [n.id],
        explain: 'Per-task rows: median ' + fmtNum(m.tasks.median) + ', max ' + fmtNum(m.tasks.max) + ' over ' + (m.tasks.count || '?') + ' tasks. A few key values hold most of the rows, so one task does most of the work while the others wait.' + (conf(ctx.res, 'spark.sql.adaptive.skewJoin.enabled') === 'false' ? ' AQE skew-join handling is turned off in this run (spark.sql.adaptive.skewJoin.enabled=false).' : ''),
        why: 'The stage takes as long as its slowest task. Skewed tasks also spill to disk and can run out of memory.',
        fix: skewFix(key) });
    });
    ctx.main.filter((n) => n.attrs.skew === true || (n.attrs.readMode && /skewed/.test(n.attrs.readMode))).slice(0, 1).forEach((n) => out.push({ severity: 'info', title: 'AQE split skewed partitions', nodes: ctx.main.filter((x) => x.attrs.skew === true || (x.attrs.readMode && /skewed/.test(x.attrs.readMode))).map((x) => x.id),
      explain: 'This join is marked skew=true (or its shuffle read is "skewed"): AQE found oversized partitions at runtime and split them into smaller tasks. The skew is real, but Spark is already handling it.',
      why: 'Good news. If the stage is still slow, the hot key may be too big even after splitting; salting the key or filtering out the hot value (often NULL or a default) helps further.', fix: skewFix(cols(n.attrs.leftKeys).join(', ')) }));
    out.forEach((f) => delete f._key);
    return out;
  });
  function skewFix(key) {
    const k = (key || 'key').split(',')[0].trim() || 'key';
    return { pyspark: '# Salting: spread the hot key over N buckets\nfrom pyspark.sql import functions as F\nN = 16\nfact_s = fact.withColumn("salt", (F.rand() * N).cast("int"))\ndim_s = dim.crossJoin(spark.range(N).withColumnRenamed("id", "salt"))\nresult = fact_s.join(dim_s, ["' + k + '", "salt"]).drop("salt")', sql: '-- Spark 3.x+: ask AQE to split skewed partitions of this join\nSELECT /*+ SHUFFLE_MERGE(dim) */ ...  -- keep AQE skew join on (below)\n\n-- or isolate the hot key: handle ' + k + ' = <hot value> separately and UNION ALL', config: 'spark.sql.adaptive.enabled=true\nspark.sql.adaptive.skewJoin.enabled=true\nspark.sql.adaptive.skewJoin.skewedPartitionFactor=5\nspark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=256MB' };
  }

  // ---------- scans and filters ----------
  function ancestors(ctx, n, max) { const out = []; let p = n.parentId && ctx.byId[n.parentId]; while (p && out.length < (max || 6)) { out.push(p); p = p.parentId && ctx.byId[p.parentId]; } return out; }
  function filtersAbove(ctx, scan) {
    const out = []; for (const a of ancestors(ctx, scan, 6)) { if (a.category === 'filter') out.push(a); else if (!/^(ColumnarToRow|Project|BatchEvalPython|ArrowEvalPython|InputAdapter|PhotonProject)$/.test(a.name) && a.category !== 'udf' && a.category !== 'stage') break; }
    return out;
  }
  function udfInputs(ctx, scan) { const out = []; ancestors(ctx, scan, 4).forEach((a) => { if (a.category === 'udf') (a.attrs.udfs || []).forEach((u) => out.push(u)); }); return out; }
  const INFERRED = /^\(?(isnotnull|IsNotNull)\([^()]*\)\)?$|^\(?size\([^()]*\) > 0\)?$/;
  const nontrivial = (list) => (list || []).filter((p) => !INFERRED.test(p.trim()));
  // Strip filters Spark adds by itself (null checks, size(arr) > 0 before explode); true if anything real is left.
  const realCondition = (c) => /\w#\d+|pythonUDF/.test(String(c || '').replace(/isnotnull\([^()]*\)/gi, '').replace(/\(?size\([^()]*\) > 0\)?/g, ''));
  function blocker(expr) {
    const e = expr || '';
    if (/pythonUDF\d*|EvalPython|\bUDF\b|udf\(/i.test(e)) return 'a Python UDF';
    if (/\bcast\(/i.test(e)) return 'a cast on the column';
    if (/<=> true|<=> false/.test(e)) return 'a null-safe comparison with true (<=>), which F.when(...)/CASE booleans produce';
    if (/\b(lower|upper|substring|substr|trim|concat|regexp\w*|date_format|to_date|year|month|coalesce)\(/i.test(e)) return 'a function wrapped around the column';
    return null;
  }
  rule('partition-pruning', 'Partition pruning', (ctx) => {
    const out = [];
    ctx.main.filter((n) => n.category === 'scan' && Array.isArray(n.attrs.partitionFilters) && n.attrs.partitionFilters.length === 0).forEach((n) => {
      const { partCols } = P.scanParts(n); if (!partCols.length) return;
      const tbl = tableOf(n) || 'table';
      const fl = filtersAbove(ctx, n); const uin = udfInputs(ctx, n);
      const hits = fl.filter((f) => partCols.some((c) => new RegExp('\\b' + c + '#').test(f.attrs.condition || '')) || (/pythonUDF/.test(f.attrs.condition || '') && uin.some((u) => partCols.some((c) => new RegExp('\\b' + c + '#').test(u)))));
      const pc = partCols.join(', ');
      const fix = { pyspark: '# Filter the partition column directly, with a plain comparison\nfrom pyspark.sql import functions as F\ndf = spark.read.parquet(".../' + tbl + '").filter(F.col("' + partCols[0] + '") >= "2026-01-01")', sql: 'SELECT ... FROM ' + tbl + ' WHERE ' + partCols[0] + " >= '2026-01-01'", config: '# no config needed; pruning happens automatically when the predicate is on the partition column only' };
      if (hits.length) {
        const why = blocker(hits.map((h) => h.attrs.condition).join(' ') + ' ' + uin.join(' ')) || 'the expression';
        out.push({ severity: 'high', title: 'Filter on partition column ' + pc + ' does not prune partitions of ' + tbl, nodes: [n.id].concat(hits.map((h) => h.id)),
          explain: 'The query filters on ' + pc + ', which is how ' + tbl + ' is partitioned on disk, but PartitionFilters is empty: the filter goes through ' + why + ', so Spark cannot evaluate it from folder names and reads every partition first.',
          why: 'Partition pruning skips whole folders before any file is opened. Without it the job reads the full table and throws most rows away.', fix });
      } else {
        const prev = out.find((f) => f._low && f._tbl === tbl); if (prev) { prev.nodes.push(n.id); return; }
        out.push({ _low: true, _tbl: tbl, severity: 'low', title: 'Reads every ' + pc + ' partition of ' + tbl, nodes: [n.id],
          explain: tbl + ' is partitioned by ' + pc + ' and this scan has PartitionFilters: [] — all partitions are read. That is right for a full rebuild, but most reports only need a date range.',
          why: 'A filter on the partition column skips whole folders before any file is opened.', fix });
      }
    });
    out.forEach((f) => { delete f._low; delete f._tbl; });
    return out;
  });
  rule('pushdown', 'Filters not pushed to the scan', (ctx) => {
    const out = [];
    ctx.main.filter((n) => n.category === 'scan' && /^(FileScan|Scan|BatchScan|PhotonScan)/.test(n.name)).forEach((n) => {
      const a = n.attrs; const tbl = tableOf(n) || 'table';
      const fl = filtersAbove(ctx, n); const cond = fl.map((f) => f.attrs.condition || '').join(' AND ');
      const data = nontrivial(a.dataFilters); const pushed = nontrivial(a.pushedFilters);
      const condPreds = cond ? cond : data.join(' AND ');
      const bl = blocker(condPreds);
      if (!fl.length && !data.length) return;
      if (a.pushedFilters == null) return;
      if (!realCondition(condPreds)) return;
      // A filter on partition columns only is the partition-pruning rule's business.
      const { partCols } = P.scanParts(n); const uin = udfInputs(ctx, n);
      const refs = (T.stripIds ? (condPreds + ' ' + (/pythonUDF/.test(condPreds) ? uin.join(' ') : '')) : condPreds).match(/(\w+)#\d+/g) || [];
      const refCols = [...new Set(refs.map((r) => r.replace(/#\d+$/, '')).filter((c) => !/^pythonUDF\d*$/.test(c)))];
      if (refCols.length && partCols.length && refCols.every((c) => partCols.includes(c))) return;
      if (pushed.length && !bl) return;
      if (!data.length && !bl && !/pythonUDF/.test(cond)) return;
      const reason = bl || 'an expression the data source cannot evaluate';
      const fmt = (a.format || '').toLowerCase();
      out.push({ key: 'pushdown|' + tbl, severity: 'medium', title: 'Filter on ' + tbl + ' is not pushed into the scan (' + reason + ')', nodes: [n.id].concat(fl.map((f) => f.id)),
        explain: 'The rows are filtered by ' + T.stripIds(condPreds).slice(0, 220) + ' only after they are read. PushedFilters ' + (pushed.length ? 'has ' + pushed.join(', ') + ' but not the rest' : 'is empty apart from null checks') + ', because the condition uses ' + reason + '.' + (/cast/.test(reason) && /csv|json|text/.test(fmt) ? ' With a ' + fmt.toUpperCase() + ' source read without a schema every column is a string, so comparing with a number adds a cast.' : ''),
        why: 'Pushed filters let ' + (/parquet|orc|delta/.test(fmt) ? 'Parquet/ORC skip whole row groups using min/max statistics' : 'the reader drop rows while parsing') + ' instead of materializing every row first.',
        fix: { pyspark: /<=>/.test(reason) ? '# Filter on the source column, not on a label derived from it\nfrom pyspark.sql import functions as F\ndf.filter(F.col("amount") > 250)  # pushed to Parquet\n# instead of: withColumn("tier", F.when(F.col("amount") > 250, "high")...).filter(F.col("tier") == "high")' : /cast/.test(reason) ? '# Read with a schema so the column already has the right type\nschema = "event_id STRING, user_id INT, event_type STRING, ts TIMESTAMP"\ndf = spark.read.schema(schema).option("header", True).csv(".../' + tbl + '")\ndf.filter((F.col("user_id") == 42) | (F.col("event_type") == "buy"))' : /UDF/.test(reason) ? '# Replace the UDF in the filter with built-in column functions\nfrom pyspark.sql import functions as F\ndf.filter(F.col("order_date") >= "2026-02-20")' : '# Compare the raw column, move the function to the literal side\ndf.filter(F.col("name") == "alice")  # instead of F.lower(col) == "alice" when data is already normalized', sql: '-- compare the column itself, not a function of it\nWHERE user_id = 42  -- column already typed INT', config: 'spark.sql.parquet.filterPushdown=true\nspark.sql.csv.filterPushdown.enabled=true' } });
    });
    return out;
  });
  rule('python-udf', 'Python UDFs', (ctx) => ctx.main.filter((n) => n.category === 'udf').map((n) => {
    const arrow = !/^BatchEvalPython/.test(n.name); const names = (n.attrs.udfs || []).map((u) => (/^(\w+)\(/.exec(T.stripIds(u)) || [])[1] || u).slice(0, 4).join(', ');
    return { severity: arrow ? 'low' : 'high', title: (arrow ? 'Arrow / pandas UDF' : 'Row-at-a-time Python UDF') + (names ? ' ' + names : '') + ' (' + n.name + ')', nodes: [n.id],
      explain: arrow ? 'Batches of rows are converted to Arrow, sent to a Python worker, processed with pandas and sent back. This is vectorized and far faster than a plain @udf, but data still leaves the JVM.' : 'Every row is pickled, sent to a Python worker process, run through the function and sent back. The JVM cannot optimize, push down or code-generate anything inside it.',
      why: arrow ? 'Fine for logic Spark has no built-in for. If a built-in function exists, it is still faster and lets the optimizer see the logic.' : 'Plain Python UDFs are often 10–100× slower than built-in functions, and filters on their output cannot be pushed into the scan.',
      fix: { pyspark: '# Prefer built-in functions\nfrom pyspark.sql import functions as F\ndf = df.withColumn("tier", F.when(F.col("amount") >= 500, "high").when(F.col("amount") >= 100, "mid").otherwise("low"))\n\n# If Python is unavoidable, vectorize it\nimport pandas as pd\n@F.pandas_udf("string")\ndef tier(amount: pd.Series) -> pd.Series:\n    return pd.cut(amount, [-1, 100, 500, float("inf")], labels=["low", "mid", "high"]).astype(str)', sql: "CASE WHEN amount >= 500 THEN 'high' WHEN amount >= 100 THEN 'mid' ELSE 'low' END AS tier", config: '# Spark 3.5+/4: run plain @udf through Arrow as well\nspark.sql.execution.pythonUDF.arrow.enabled=true' } };
  }));
  rule('wide-schema', 'Wide ReadSchema', (ctx) => ctx.main.filter((n) => n.category === 'scan' && ((n.attrs.readCols || []).length >= 20 || (n.attrs.readSchemaTruncated && (n.attrs.readCols || []).length >= 10))).map((n) => {
    const c = n.attrs.readCols.length; const tbl = tableOf(n) || 'table';
    return { severity: c >= 50 ? 'medium' : 'low', title: tbl + ': reads ' + (n.attrs.readSchemaTruncated ? 'at least ' : '') + c + ' columns', nodes: [n.id],
      explain: 'ReadSchema lists the columns the scan actually decodes. Columnar formats read only those columns, so every extra column is extra I/O and memory.' + (n.attrs.readSchemaTruncated ? ' (The schema was cut off with "..." in the plan, so there are more.)' : ''),
      why: 'Selecting only the needed columns early (instead of select("*") or carrying whole rows through joins) often cuts scan time in proportion.',
      fix: { pyspark: 'df = spark.table("' + tbl + '").select("id", "amount", "order_date")  # only what the query uses', sql: 'SELECT id, amount, order_date FROM ' + tbl + '  -- instead of SELECT *', config: '# Parquet nested-column pruning (on by default in 3.x)\nspark.sql.optimizer.nestedSchemaPruning.enabled=true' } };
  }));
  rule('repeated-scan', 'Same table scanned more than once', (ctx) => {
    const groups = {};
    ctx.main.filter((n) => n.category === 'scan' && /^(FileScan|Scan|BatchScan|PhotonScan)/.test(n.name)).forEach((n) => { const k = (n.attrs.location && n.attrs.location.paths && n.attrs.location.paths.join(',')) || n.attrs.table; if (!k) return; (groups[k] = groups[k] || []).push(n); });
    return Object.keys(groups).filter((k) => groups[k].length >= 2).map((k) => {
      const ns = groups[k]; const tbl = tableOf(ns[0]) || k;
      return { severity: ns.length >= 3 ? 'medium' : 'low', title: tbl + ' is scanned ' + ns.length + ' times', nodes: ns.map((n) => n.id),
        explain: 'The same files are read by ' + ns.length + ' separate scans (a self-join, a union of the same source, or a subquery on the same table). Spark reuses an exchange only when the subtrees are identical; these differ (different columns or filters), so each reads the files again.',
        why: 'Each scan pays the full I/O again. If the data is reused in the same job, read it once.',
        fix: { pyspark: 'base = spark.table("' + tbl + '").select("needed", "cols")\nbase.cache()  # or .persist(); call base.unpersist() when done\na = base.filter(...)\nb = base.groupBy(...).agg(...)', sql: 'CACHE TABLE ' + tbl + '_c AS SELECT needed, cols FROM ' + tbl + ';\n-- or compute both results in one pass with conditional aggregation:\nSELECT sum(CASE WHEN ... THEN amount END), count(*) FROM ' + tbl, config: '# exchange reuse is on by default\nspark.sql.exchange.reuse=true' } };
    });
  });
  // ---------- operators ----------
  rule('window-no-partition', 'Window without partitionBy', (ctx) => ctx.main.filter((n) => n.category === 'window' && n.attrs.partitionSpec && n.attrs.partitionSpec.length === 0).map((n) => ({
    severity: 'high', title: 'Window function without partitionBy runs in one task', nodes: [n.id],
    explain: 'A window with only orderBy (for example row_number() over (order by ts)) needs every row in one partition to number them in order. Spark moves the whole data set to a single task (Exchange SinglePartition).',
    why: 'One task on one core processes everything and must hold it in memory; it is slow and the classic cause of out-of-memory on a single executor.',
    fix: { pyspark: 'from pyspark.sql import Window, functions as F\nw = Window.partitionBy("customer_id").orderBy("order_date")\ndf = df.withColumn("rn", F.row_number().over(w))\n\n# need a global unique id, not an ordered rank?\ndf = df.withColumn("id", F.monotonically_increasing_id())', sql: 'row_number() OVER (PARTITION BY customer_id ORDER BY order_date)', config: '# no config fixes this; add a partition key' } })));
  rule('global-sort', 'Global sort', (ctx) => {
    const out = [];
    ctx.main.filter((n) => /^(Sort|PhotonSort)$/.test(n.name)).forEach((n) => {
      const parent = n.parentId && ctx.byId[n.parentId];
      const underWrite = (x) => x && (x.category === 'write' || x.name === 'WriteFiles');
      if (underWrite(parent) || (parent && parent.category === 'stage' && underWrite(ctx.byId[parent.parentId]))) {
        out.push({ severity: 'low', title: 'Sort before write by ' + cols(n.attrs.sortOrder).join(', '), nodes: [n.id],
          explain: 'Spark sorts each task\'s rows by the output partition columns (and bucket / sort columns) before writing, so every task writes one file per partition at a time.',
          why: 'Needed for partitioned writes, but costly when there are many small partitions per task. Repartitioning by the partition column first gives one file per partition and makes this sort cheap.',
          fix: { pyspark: 'df.repartition("order_date").write.partitionBy("order_date").parquet(".../out")', sql: 'INSERT OVERWRITE ... SELECT /*+ REPARTITION(order_date) */ ...', config: '# Spark 3.4+: planned write ordering (on by default)\nspark.sql.optimizer.plannedWrite.enabled=true' } });
      } else if (n.attrs.global === true) {
        out.push({ severity: 'medium', title: 'Global sort (ORDER BY) on ' + cols(n.attrs.sortOrder).join(', '), nodes: [n.id],
          explain: 'A full ORDER BY samples the data to pick range boundaries, shuffles everything by range, then sorts each partition. That is an extra sampling job plus a full shuffle and sort.',
          why: 'Often only the top rows are needed, or the order is lost anyway once the data is written or joined.',
          fix: { pyspark: '# only the top N rows?\ndf.orderBy(F.desc("revenue")).limit(100)   # becomes TakeOrderedAndProject, no full sort\n\n# order inside partitions is enough (e.g. before writing)?\ndf.sortWithinPartitions("amount")', sql: 'SELECT ... ORDER BY revenue DESC LIMIT 100\n-- or SORT BY amount (per-partition)', config: '# none; drop the ORDER BY if nothing reads the order' } });
      }
    });
    return out;
  });
  rule('expand', 'Expand from count(DISTINCT) / grouping sets', (ctx) => ctx.main.filter((n) => /^(Expand|PhotonExpand)$/.test(n.name) && (n.attrs.projections || 0) >= 2).map((n) => {
    const aggs = []; ctx.main.filter((x) => x.category === 'agg').forEach((x) => (x.attrs.functions || []).forEach((f) => { if (/distinct/i.test(f)) aggs.push(T.stripIds(f)); }));
    return { severity: 'medium', title: 'Expand multiplies rows ×' + n.attrs.projections + (aggs.length ? ' for ' + [...new Set(aggs)].slice(0, 2).join(', ') : ''), nodes: [n.id],
      explain: 'Every input row is copied ' + n.attrs.projections + ' times (one copy per distinct aggregate or grouping set), then aggregated twice. This is how Spark runs several count(DISTINCT ...) together, or a DISTINCT next to other aggregates, ROLLUP or CUBE.',
      why: 'The shuffle after Expand carries ' + n.attrs.projections + '× the rows. On large tables this is often the slowest stage of the query.',
      fix: { pyspark: '# An estimate is usually enough (error ~1-5%)\nfrom pyspark.sql import functions as F\ndf.groupBy("channel").agg(F.approx_count_distinct("customer_id").alias("customers"), F.sum("amount"))', sql: 'SELECT channel, approx_count_distinct(customer_id), sum(amount) FROM orders GROUP BY channel', config: '# or compute each exact distinct count separately and join the small results' } };
  }));
  rule('single-partition', 'coalesce(1) / repartition(1)', (ctx) => ctx.main.filter((n) => (n.name === 'Coalesce' && n.attrs.num === 1) || (P.isShuffle(n) && n.attrs.partitioning && ((n.attrs.partitioning.type === 'single' && /REPARTITION/.test(n.attrs.shuffleOrigin || '')) || (n.attrs.partitioning.type === 'roundrobin' && n.attrs.partitioning.num === 1)))).map((n) => {
    const co = n.name === 'Coalesce';
    return { severity: 'high', title: (co ? 'coalesce(1)' : 'repartition(1)') + ' squeezes the data into a single task', nodes: [n.id],
      explain: co ? 'coalesce(1) merges partitions without a shuffle, which also pulls the whole upstream stage (scan, sort, everything up to the last shuffle) into one task.' : 'repartition(1) shuffles every row to a single partition, so everything after it runs in one task.',
      why: 'One core does all the work and one executor must hold all the data. Usually done to get a single output file; there are cheaper ways.',
      fix: { pyspark: co ? '# Shuffle first so the heavy work stays parallel, then write one file\ndf.repartition(1).write.parquet(".../out")\n\n# or keep several files (best)\ndf.write.option("maxRecordsPerFile", 5_000_000).parquet(".../out")' : '# Let Spark write a few files in parallel\ndf.repartition(8).write.parquet(".../out")\n# need one CSV for a person? write normally and merge with a small step, or collect only small results with toPandas()', sql: 'SELECT /*+ REPARTITION(8) */ * FROM ...', config: 'spark.sql.files.maxRecordsPerFile=5000000' } };
  }));
  rule('aqe-off', 'Adaptive Query Execution is off', (ctx) => {
    const off = conf(ctx.res, 'spark.sql.adaptive.enabled') === 'false' || (!ctx.res.aqe.present && ctx.main.some((n) => P.isExchange(n)) && ctx.res.format !== 'codegen' && !ctx.res.nodes.some((n) => /^Photon/.test(n.name)) && !/^Execute /.test(ctx.res.physical ? ctx.res.physical.name : ''));
    if (!off) return;
    return { severity: 'medium', title: 'Adaptive Query Execution (AQE) is off', nodes: [],
      explain: 'The plan has no AdaptiveSparkPlan node, so Spark fixed every join strategy and partition count before seeing any data.' + (conf(ctx.res, 'spark.sql.adaptive.enabled') === 'false' ? ' The event log confirms spark.sql.adaptive.enabled=false.' : ' (AQE is on by default since Spark 3.2; some plans for cached data or commands also omit it.)'),
      why: 'AQE switches joins to broadcast when a side turns out small, merges tiny shuffle partitions and splits skewed ones, all at runtime.',
      fix: { pyspark: 'spark.conf.set("spark.sql.adaptive.enabled", "true")', sql: 'SET spark.sql.adaptive.enabled=true;', config: 'spark.sql.adaptive.enabled=true\nspark.sql.adaptive.coalescePartitions.enabled=true\nspark.sql.adaptive.skewJoin.enabled=true' } };
  });
  rule('collect-explode', 'collect_list / explode', (ctx) => {
    const col = ctx.main.filter((n) => n.category === 'agg' && (n.attrs.functions || []).some((f) => /collect_(list|set)|array_agg/.test(f)) && !(n.attrs.functions || []).every((f) => /partial_/.test(f)));
    const gen = ctx.main.filter((n) => /^(Generate|PhotonGenerate)$/.test(n.name) && /explode|inline|posexplode/.test(n.attrs.generator || ''));
    if (!col.length && !gen.length) return;
    const both = col.length && gen.length;
    return { severity: both ? 'medium' : 'low', title: both ? 'explode() then collect_list(): rows are blown up and packed back' : col.length ? 'collect_list / collect_set builds large arrays' : 'explode() multiplies rows', nodes: col.concat(gen).map((n) => n.id),
      explain: (gen.length ? 'explode turns each array element into its own row, so the data after it can be many times larger. ' : '') + (col.length ? 'collect_list gathers all values of a group into one array held in memory, using ObjectHashAggregate (or a sort-based fallback).' : '') + (both ? ' Doing both usually means an array is being transformed element by element, which higher-order functions do without leaving the row.' : ''),
      why: 'Big groups make huge arrays (memory pressure, spill); explode multiplies shuffle volume.',
      fix: { pyspark: '# Transform arrays in place with higher-order functions\nfrom pyspark.sql import functions as F\ndf = df.withColumn("tags2", F.transform("tags", lambda t: t * 2))\ndf = df.withColumn("big_tags", F.filter("tags", lambda t: t > 5))\n# aggregate instead of collecting when you only need a number\ndf.groupBy("customer_id").agg(F.count("*"), F.countDistinct("tag"))', sql: 'SELECT transform(tags, t -> t * 2), filter(tags, t -> t > 5), aggregate(tags, 0, (acc, t) -> acc + t) FROM orders', config: '# for very large collect_list groups\nspark.sql.objectHashAggregate.sortBased.fallbackThreshold=128' } };
  });
  rule('text-format', 'CSV / JSON source', (ctx) => ctx.main.filter((n) => n.category === 'scan' && /^(csv|json|text)$/i.test(n.attrs.format || '')).map((n) => {
    const fmt = (n.attrs.format || '').toUpperCase(); const tbl = tableOf(n) || 'source';
    return { severity: 'medium', title: tbl + ' is read from ' + fmt, nodes: [n.id],
      explain: fmt + ' has no column pruning (every byte is parsed even if only one column is used), no row-group statistics for filter skipping and no stored types. Batched: false means no vectorized reader.',
      why: 'Converting once to Parquet or Delta usually makes later reads 5–20× faster and much smaller.',
      fix: { pyspark: '# Convert once, then query the Parquet copy\nschema = "event_id STRING, user_id INT, event_type STRING, ts TIMESTAMP"\n(spark.read.schema(schema).option("header", True).csv(".../' + tbl + '")\n   .write.mode("overwrite").partitionBy("event_type").parquet(".../' + tbl + '_parquet"))', sql: 'CREATE TABLE ' + tbl + '_pq USING parquet AS SELECT * FROM ' + tbl + ';', config: '# never infer the schema on large text data in production\n# (it reads the data an extra time): pass .schema(...) instead of inferSchema=true' } };
  }));
  // ---------- metrics (event log) ----------
  rule('small-files', 'Many small files', (ctx) => ctx.main.filter((n) => n.category === 'scan').map((n) => {
    const files = metric(n, 'number of files read'), size = metric(n, 'size of files read');
    if (!files || !size || files.value < 50) return null; const avg = size.value / files.value; if (avg >= 32 * 1048576) return null;
    const tbl = tableOf(n) || 'table';
    return { severity: avg < 4 * 1048576 ? 'medium' : 'low', title: tbl + ': ' + fmtNum(files.value) + ' files averaging ' + fmtBytes(avg), nodes: [n.id],
      explain: 'This scan opened ' + fmtNum(files.value) + ' files for ' + fmtBytes(size.value) + ' in total. Each file costs a listing entry, an open, a footer read and scheduling overhead.',
      why: 'Files of 128MB–1GB read much faster than thousands of tiny ones. Small files usually come from writing with too many partitions or frequent small appends.',
      fix: { pyspark: '# Compact: rewrite with fewer, larger files\n(spark.read.parquet(".../' + tbl + '")\n   .repartition("order_date")\n   .write.mode("overwrite").partitionBy("order_date").parquet(".../' + tbl + '_compacted"))', sql: '-- Delta Lake\nOPTIMIZE ' + tbl + ';', config: '# pack small files into bigger read tasks\nspark.sql.files.maxPartitionBytes=256MB\nspark.sql.files.openCostInBytes=8MB' } };
  }).filter(Boolean));
  rule('spill', 'Spill to disk', (ctx) => ctx.main.filter((n) => { const m = metric(n, 'spill size'); return m && m.value > 0; }).map((n) => {
    const m = metric(n, 'spill size');
    return { severity: m.value > 1024 * 1048576 ? 'high' : 'medium', title: n.name + ' spilled ' + fmtBytes(m.value) + ' to disk', nodes: [n.id],
      explain: 'The operator ran out of execution memory and wrote intermediate data to local disk, then read it back.' + (m.tasks && m.tasks.max ? ' The worst task spilled ' + fmtBytes(m.tasks.max) + '.' : ''),
      why: 'Spill is slow (serialize, write, read, merge). It often points at skew or too few shuffle partitions.',
      fix: { pyspark: 'spark.conf.set("spark.sql.shuffle.partitions", "400")  # more, smaller partitions', sql: 'SET spark.sql.adaptive.advisoryPartitionSizeInBytes=64MB;', config: 'spark.sql.shuffle.partitions=400\nspark.executor.memory=8g\nspark.memory.fraction=0.6' } };
  }));
  rule('big-broadcast', 'Large broadcast', (ctx) => ctx.main.filter((n) => n.category === 'broadcast').map((n) => { const m = metric(n, 'data size'); if (!m || m.value < 256 * 1048576) return null; return { severity: 'medium', title: 'Broadcast of ' + fmtBytes(m.value), nodes: [n.id], explain: 'This broadcast is ' + fmtBytes(m.value) + '. It is collected to the driver and copied to every executor.', why: 'Large broadcasts risk driver out-of-memory and long broadcast timeouts.', fix: { pyspark: '# let Spark shuffle instead\nresult = big.join(medium.hint("merge"), "key")', sql: 'SELECT /*+ MERGE(medium) */ ...', config: 'spark.sql.autoBroadcastJoinThreshold=10MB\nspark.sql.broadcastTimeout=600' } }; }).filter(Boolean));
  // ---------- Photon ----------
  rule('photon-fallback', 'Photon fallback', (ctx) => {
    const hasPhoton = ctx.res.nodes.some((n) => P.isPhoton(n.name)); if (!hasPhoton) return;
    const fb = ctx.main.filter((n) => !P.isPhoton(n.name) && !/^(AdaptiveSparkPlan|ColumnarToRow|RowToColumnar|WholeStageCodegen|InputAdapter|ReusedExchange|ResultQueryStage|ShuffleQueryStage|BroadcastQueryStage|AQEShuffleRead|Plans)$/.test(n.name) && n.category !== 'marker' && n.category !== 'subquery' && !/QueryStage$/.test(n.name) && !(/^Execute /.test(n.name)));
    if (!fb.length) return { severity: 'info', title: 'Runs fully in Photon', nodes: [], explain: 'Every operator in this plan is a Photon operator.', why: 'Nothing to change.', fix: null };
    const names = [...new Set(fb.map((n) => n.name))];
    const expl = ctx.res.photon && ctx.res.photon.explanation ? ' Photon says: ' + ctx.res.photon.explanation.split('\n').filter((l) => l.trim()).slice(0, 3).join(' ').slice(0, 300) : '';
    return { severity: fb.some((n) => n.category === 'udf') || fb.length > 2 ? 'medium' : 'low', title: 'Photon falls back to Spark for ' + names.slice(0, 4).join(', '), nodes: fb.map((n) => n.id),
      explain: 'These operators are not supported by Photon, so data is converted from Photon\'s columnar format to rows (ColumnarToRow) and back (PhotonRowToColumnar) around them.' + expl,
      why: 'Each switch costs a format conversion and the fallback operator runs at regular Spark speed.',
      fix: { pyspark: '# Replace Python UDFs with built-in functions so Photon can run them\nfrom pyspark.sql import functions as F\ndf = df.withColumn("tier", F.when(F.col("amount") >= 500, "high").otherwise("low"))', sql: '-- check unsupported expressions:\nEXPLAIN ' + 'SELECT ...  -- the "== Photon Explanation ==" section names them', config: '# Databricks\nspark.databricks.photon.enabled=true' } };
  });

  // ---------- entry ----------
  function diagnose(res) {
    if (!res || !res.ok) return { findings: [], summary: summarize(res, []) };
    const byId = {}; res.nodes.forEach((n) => { byId[n.id] = n; }); BYID = byId;
    attachCostSizes(res);
    const ctx = { res, main: res.nodes.filter((n) => n.category !== 'marker'), byId };
    let findings = [];
    RULES.forEach((r, i) => {
      let out; try { out = r.fn(ctx); } catch (e) { out = null; if (typeof console !== 'undefined') console.warn('rule ' + r.id + ' failed', e); }
      if (!out) return; (Array.isArray(out) ? out : [out]).forEach((f, k) => { if (!f) return; f.title = T.stripIds(f.title); f.rule = r.id; f.ruleTitle = r.title; f.order = i * 100 + k; f.key = f.key || (r.id + '|' + T.stripIds(f.title)); f.id = r.id + (k ? '-' + k : ''); findings.push(f); });
    });
    findings.sort(findingsSort);
    return { findings, summary: summarize(res, findings) };
  }
  function summarize(res, findings) {
    const s = { high: 0, medium: 0, low: 0, info: 0, shuffles: 0, broadcasts: 0, joins: {}, scans: 0, udfs: 0, stages: 0, score: 100 };
    findings.forEach((f) => { s[f.severity]++; });
    if (res && res.nodes) res.nodes.forEach((n) => { if (P.isShuffle(n) && n.name !== 'ReusedExchange') s.shuffles++; if (n.category === 'broadcast') s.broadcasts++; if (n.category === 'join') s.joins[n.name] = (s.joins[n.name] || 0) + 1; if (n.category === 'scan') s.scans++; if (n.category === 'udf') s.udfs++; if (n.stage != null) s.stages = Math.max(s.stages, n.stage + 1); });
    s.score = Math.max(0, 100 - 25 * s.high - 10 * s.medium - 3 * s.low);
    return s;
  }
  return { diagnose, RULES: RULES.map((r) => ({ id: r.id, title: r.title })), sideSize, tableOf };
});
