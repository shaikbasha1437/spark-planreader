/* Plain-English notes for Spark physical operators, shown on hover and in the detail panel. */
(function (root, factory) {
  const m = factory();
  if (typeof module === 'object' && module.exports) module.exports = m;
  else (root.SP = root.SP || {}).glossary = m;
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';
  const G = {
    AdaptiveSparkPlan: 'The wrapper Adaptive Query Execution (AQE) puts around a plan. With isFinalPlan=true you are looking at the plan Spark actually ran, after it re-planned using real shuffle sizes; with false it is still the first guess.',
    FileScan: 'Reads files (Parquet, ORC, CSV, JSON...). Check PartitionFilters (folders skipped), PushedFilters (row groups skipped inside files) and ReadSchema (columns actually read).',
    BatchScan: 'A DataSource V2 scan (Iceberg, Delta via V2, JDBC, Kafka...). Pushed filters and the columns read are listed in its arguments.',
    Scan: 'Reads a table or in-memory data source.',
    LocalTableScan: 'Rows that live in the driver (a literal list, createDataFrame from a Python list, or a tiny folded result). No files are read.',
    InMemoryTableScan: 'Reads a DataFrame you cached with .cache() or .persist(). The first run fills the cache; later runs read memory instead of the source.',
    InMemoryRelation: 'The cached data behind an InMemoryTableScan, with its storage level. Its child plan runs only when the cache is filled.',
    ColumnarToRow: 'Turns column batches (from a vectorized Parquet/ORC reader or Photon) into rows for the next row-based operator. Cheap, but frequent switches mean an operator could not run columnar.',
    RowToColumnar: 'Turns rows back into column batches. Seen when a columnar engine (Photon, RAPIDS) resumes after a row-only operator.',
    Filter: 'Keeps the rows that match a condition. Ideally its predicates also show up as PushedFilters on the scan below, so less data is read in the first place.',
    Project: 'Picks, renames or computes columns (a SELECT list). Usually free inside whole-stage codegen.',
    Exchange: 'A shuffle: every row is sent over the network to the partition chosen by the partitioning (hash of keys, range, round robin or a single partition). The most expensive common operator and a stage boundary.',
    ShuffleExchange: 'A shuffle (older name for Exchange).',
    BroadcastExchange: 'Collects one (small) side of a join to the driver and sends a full copy to every executor so the join needs no shuffle of the big side.',
    ReusedExchange: 'Spark noticed the same shuffle or broadcast appears twice and reuses the first one\'s output instead of computing it again. Good news.',
    ShuffleQueryStage: 'AQE: a finished shuffle. Spark ran everything below it first and measured its size before planning what is above.',
    BroadcastQueryStage: 'AQE: a finished broadcast, measured before the rest of the plan was decided.',
    ResultQueryStage: 'AQE (Spark 4): the last stage that produces the query result.',
    TableCacheQueryStage: 'AQE: a cached table materialized as its own stage.',
    AQEShuffleRead: 'AQE reads a shuffle in a smarter way: "coalesced" merges tiny partitions, "skewed" splits huge ones, "local" reads without a network fetch after a join was switched to broadcast.',
    CustomShuffleReader: 'Spark 3.0/3.1 name for AQEShuffleRead (coalesced, skewed or local shuffle reads).',
    BroadcastHashJoin: 'Join where one side is broadcast to every executor and turned into a hash table. Fast, and the big side is not shuffled. Only for sides small enough to fit in memory.',
    SortMergeJoin: 'Join that shuffles both sides by the join key, sorts them and walks them together. Scales to any size, but costs two shuffles and two sorts. If one side is small, a broadcast join is usually faster.',
    ShuffledHashJoin: 'Join that shuffles both sides by key and builds a hash table from the smaller side per partition. Avoids sorting; needs each build partition to fit in memory.',
    BroadcastNestedLoopJoin: 'Join with no usable equality key: one side is broadcast and every row is compared with every row of the other side. Can explode in cost; usually caused by a non-equi or OR condition, or a missing join key.',
    CartesianProduct: 'Every row of one side paired with every row of the other (cross join). Rows out = rows left × rows right.',
    HashAggregate: 'GROUP BY with a hash table. Usually appears twice: a partial aggregate before the shuffle (per partition) and a final aggregate after it.',
    ObjectHashAggregate: 'Hash aggregation for functions with object state such as collect_list, collect_set or percentile. Can fall back to sorting when memory runs short.',
    SortAggregate: 'GROUP BY by sorting rows first. Spark picks it when the aggregate buffer types do not fit a hash map; slower than HashAggregate.',
    Expand: 'Copies each input row several times, once per grouping set. Comes from count(DISTINCT ...) mixed with other aggregates, ROLLUP, CUBE or GROUPING SETS. Rows out = rows in × number of projections.',
    Generate: 'Turns one row into many (explode, posexplode, inline, stack, json_tuple). Watch row counts after it.',
    Window: 'Window functions (row_number, rank, lag, running sums). Rows with the same partitionBy key go to one task and are sorted by orderBy. Without partitionBy, everything goes to a single task.',
    WindowGroupLimit: 'Spark 3.5+ optimization that keeps only the top rows per window partition before the full window runs (rank <= N filters).',
    Sort: 'Sorts rows. global=true is an ORDER BY across the whole data set (needs a range shuffle first); global=false sorts inside each partition (for a sort-merge join or a window).',
    TakeOrderedAndProject: 'ORDER BY ... LIMIT n done efficiently: each partition keeps its top n, then the driver merges. Much cheaper than a full sort.',
    CollectLimit: 'LIMIT on the final result, collected to the driver.',
    GlobalLimit: 'LIMIT across all partitions (needs a single-partition exchange).',
    LocalLimit: 'LIMIT applied inside each partition first.',
    Coalesce: 'Merges partitions without a shuffle (from coalesce(n)). coalesce(1) puts the whole upstream stage into a single task.',
    Union: 'Concatenates the rows of its children (UNION ALL). No shuffle by itself.',
    Sample: 'Random sample of rows.',
    BatchEvalPython: 'Runs a row-at-a-time Python UDF (@udf). Rows are pickled and sent to a Python worker and back. Slow, and Spark cannot see inside it, so filters on its result cannot be pushed down.',
    ArrowEvalPython: 'Runs a pandas/Arrow UDF (@pandas_udf or useArrow=True) on column batches. Much faster than a plain Python UDF, but still leaves the JVM.',
    FlatMapGroupsInPandas: 'applyInPandas: every group is sent to Python as a pandas DataFrame. Each group must fit in one worker\'s memory.',
    MapInPandas: 'mapInPandas: batches of rows go through a Python function.',
    MapInArrow: 'mapInArrow: Arrow record batches go through a Python function.',
    AggregateInPandas: 'A grouped pandas UDF aggregation.',
    WindowInPandas: 'A pandas UDF used as a window function.',
    ArrowWindowPython: 'Spark 4 name for a pandas UDF window function.',
    ArrowAggregatePython: 'Spark 4 name for a pandas UDF aggregation.',
    Subquery: 'A scalar subquery computed once and plugged into the outer query as a value.',
    SubqueryBroadcast: 'Dynamic partition pruning: the filtered keys of a small dimension table are collected and used to skip partitions of the big fact table.',
    SubqueryAdaptiveBroadcast: 'AQE dynamic partition pruning: like SubqueryBroadcast, decided at runtime.',
    ReusedSubquery: 'The same subquery result reused in another place.',
    'Execute InsertIntoHadoopFsRelationCommand': 'Writes the query result as files (df.write... save / insertInto).',
    WriteFiles: 'Spark 3.4+: the step that writes output files; a Sort right below it usually sorts by partition columns.',
    'Execute CreateDataSourceTableAsSelectCommand': 'CREATE TABLE ... AS SELECT: creates a table and writes the query result.',
    WholeStageCodegen: 'Several operators fused into one generated Java function so rows stay in CPU registers. Operators marked *(n) belong to codegen stage n.',
    InputAdapter: 'Feeds rows from a non-codegen operator into a whole-stage-codegen block.',
    RDDScan: 'Reads an existing RDD.',
    Range: 'spark.range(): generates numbers, no input files.',
    PhotonScan: 'Photon (Databricks native engine) file scan.',
    PhotonGroupingAgg: 'Photon hash aggregation.',
    PhotonProject: 'Photon projection.',
    PhotonFilter: 'Photon filter.',
    PhotonBroadcastHashJoin: 'Photon broadcast hash join.',
    PhotonShuffledHashJoin: 'Photon shuffled hash join.',
    PhotonShuffleExchangeSink: 'Photon writes shuffle data (the map side of an Exchange).',
    PhotonShuffleExchangeSource: 'Photon reads shuffle data (the reduce side of an Exchange).',
    PhotonBroadcastExchange: 'Photon broadcast of a small join side.',
    PhotonResultStage: 'The last Photon stage producing the result.',
    PhotonShuffleMapStage: 'A Photon stage that ends with a shuffle write.',
    PhotonRowToColumnar: 'Leaving row-based Spark and entering Photon again; the operator above it could run in Photon, the one below could not.',
    PhotonColumnarToRow: 'Leaving Photon for a row-based Spark operator; the operator above it is not supported by Photon.',
    PhotonSort: 'Photon sort.',
    PhotonTopK: 'Photon ORDER BY ... LIMIT.',
    PhotonUnion: 'Photon union.',
    PhotonWindow: 'Photon window functions.',
    PhotonAdapter: 'Photon adapter between engines.'
  };
  function lookup(name) {
    if (!name) return null;
    if (G[name]) return G[name];
    const base = name.split(' ')[0];
    if (/^(FileScan|Scan)$/.test(base)) return G.FileScan;
    if (G[base]) return G[base];
    if (/^Execute /.test(name)) return 'Runs a command (write, DDL or cache) with the query below as input.';
    if (/Photon/.test(name)) return 'A Photon (Databricks native engine) operator.';
    return null;
  }
  return { lookup, entries: G };
});
