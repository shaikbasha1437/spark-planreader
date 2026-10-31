/* Built-in samples. Every plan except the Photon one was produced by real PySpark runs (Spark 4.0.1 and 3.5.7, local mode)
   against generated test data; paths were shortened to /data/lake. */
(function (root, factory) {
  const m = factory();
  if (typeof module === 'object' && module.exports) module.exports = m;
  else (root.SP = root.SP || {}).samples = m;
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';
  const S = [
    { id: 'smj-aqe-off', title: 'Join + groupBy + orderBy, AQE off', group: 'Joins', note: 'Spark 4.0.1 · simple' },
    { id: 'broadcast-fix', title: 'Same query with F.broadcast(customers)', group: 'Joins', note: 'Spark 4.0.1 · fix for the one above' },
    { id: 'nested-loop-join', title: 'Non-equi join (>) → BroadcastNestedLoopJoin', group: 'Joins', note: 'Spark 4.0.1' },
    { id: 'cartesian', title: 'Cross join (CartesianProduct)', group: 'Joins', note: 'Spark 4.0.1' },
    { id: 'aqe-skew-final', title: 'Skewed join, AQE final plan (skew=true)', group: 'Joins', note: 'Spark 4.0.1 · after running' },
    { id: 'spark35-aqe-skew-final', title: 'Skewed join, AQE final plan', group: 'Joins', note: 'Spark 3.5.7 · after running' },
    { id: 'aqe-final-broadcast', title: 'AQE final plan: date filter + runtime broadcast', group: 'Joins', note: 'Spark 4.0.1 · final plan' },
    { id: 'python-udf', title: 'Python @udf in a filter', group: 'UDFs', note: 'Spark 4.0.1' },
    { id: 'pandas-udf', title: '@pandas_udf (ArrowEvalPython)', group: 'UDFs', note: 'Spark 4.0.1' },
    { id: 'partition-udf', title: 'UDF on the partition column blocks pruning', group: 'Scans', note: 'Spark 4.0.1' },
    { id: 'csv-cast', title: 'CSV without a schema, cast in filter', group: 'Scans', note: 'Spark 4.0.1' },
    { id: 'cached', title: 'Cached DataFrame (InMemoryTableScan)', group: 'Scans', note: 'Spark 4.0.1' },
    { id: 'window-no-partition', title: 'row_number() over orderBy only', group: 'Operators', note: 'Spark 4.0.1' },
    { id: 'count-distinct', title: 'Two count(DISTINCT) → Expand', group: 'Operators', note: 'Spark 4.0.1' },
    { id: 'sort-coalesce', title: 'orderBy + coalesce(1)', group: 'Operators', note: 'Spark 4.0.1' },
    { id: 'repartition-one', title: 'repartition(1) + sortWithinPartitions', group: 'Operators', note: 'Spark 4.0.1' },
    { id: 'explode-collect', title: 'explode then collect_list', group: 'Operators', note: 'Spark 4.0.1' },
    { id: 'subquery-dpp', title: 'CTE self-join + scalar subquery', group: 'Operators', note: 'Spark 4.0.1' },
    { id: 'reused-exchange', title: 'ReusedExchange, AQE off', group: 'Operators', note: 'Spark 4.0.1' },
    { id: 'star-join-extended', title: 'Star join, EXPLAIN EXTENDED', group: 'Formats', note: 'Spark 4.0.1 · parsed/analyzed/optimized/physical' },
    { id: 'star-join-formatted', title: 'Star join, EXPLAIN FORMATTED', group: 'Formats', note: 'Spark 4.0.1' },
    { id: 'spark35-formatted', title: 'Star join, EXPLAIN FORMATTED', group: 'Formats', note: 'Spark 3.5.7' },
    { id: 'formatted-aqe-off', title: 'FORMATTED with AQE off', group: 'Formats', note: 'Spark 4.0.1' },
    { id: 'formatted-final', title: 'FORMATTED after running (query stage statistics)', group: 'Formats', note: 'Spark 4.0.1' },
    { id: 'cost-stats', title: 'EXPLAIN COST (size statistics)', group: 'Formats', note: 'Spark 4.0.1 · catalog table' },
    { id: 'sql-explain', title: 'SQL EXPLAIN statement output', group: 'Formats', note: 'Spark 4.0.1 · spark.sql("EXPLAIN ...").show()' },
    { id: 'ctas-command', title: 'CREATE TABLE AS SELECT (command plan)', group: 'Formats', note: 'Spark 4.0.1' },
    { id: 'photon-illustrative', title: 'Photon plan with a Python UDF fallback', group: 'Formats', note: 'Hand-written illustration, not generated (no Databricks run)' },
    { id: 'eventlog', title: 'Event log: skewed join + Python UDF (with metrics)', group: 'Event log', note: 'Spark 4.0.1 · spark.eventLog.enabled, trimmed', file: 'eventlog.json' }
  ];
  const FIX_PAIRS = [
    { id: 'broadcast', title: 'Sort-merge join → broadcast join', before: 'smj-aqe-off', after: 'broadcast-fix', code: 'orders.join(F.broadcast(customers), "customer_id")' },
    { id: 'native', title: 'Python UDF → built-in when()', before: 'python-udf', after: 'fixes/native', code: 'F.when(F.col("amount") > 250, "high").otherwise("low")' },
    { id: 'partition-filter', title: 'UDF filter → plain partition filter', before: 'partition-udf', after: 'fixes/partition_filter', code: 'orders.filter(F.col("order_date") >= "2026-02-20")' },
    { id: 'partitioned', title: 'Window → Window.partitionBy', before: 'window-no-partition', after: 'fixes/partitioned', code: 'Window.partitionBy("customer_id").orderBy("order_date")' },
    { id: 'approx', title: 'count(DISTINCT) → approx_count_distinct', before: 'count-distinct', after: 'fixes/approx', code: 'F.approx_count_distinct("customer_id")' },
    { id: 'typed', title: 'CSV string cast → read with a schema', before: 'csv-cast', after: 'fixes/typed', code: 'spark.read.schema("... user_id INT ...").csv(...)' },
    { id: 'no-coalesce', title: 'Drop coalesce(1)', before: 'sort-coalesce', after: 'fixes/no_coalesce', code: 'orders.orderBy("amount")  # no .coalesce(1)' },
    { id: 'salted', title: 'Skewed join → salted join', before: 'aqe-skew-final', after: 'fixes/salted', code: 'fact.withColumn("salt", (F.rand()*8).cast("int")).join(dim.crossJoin(spark.range(8)...), ["k", "salt"])' }
  ];
  const urlFor = (id) => { const s = S.find((x) => x.id === id); return 'samples/' + (s && s.file ? s.file : id + '.txt'); };
  return { list: S, fixPairs: FIX_PAIRS, urlFor };
});
