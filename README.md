# Spark PlanReader

**Live app: https://shaikbasha1437.github.io/spark-planreader/**

Paste Spark `EXPLAIN` output or open a Spark event log. PlanReader shows what the query really does and what is likely making it slow, with fixes you can copy. It runs entirely in your browser: nothing is uploaded, and it works offline after the first visit.

## What it reads

- **EXPLAIN in every mode**: `simple`, `extended` (parsed / analyzed / optimized logical plans plus the physical plan), `formatted` (operator list plus per-node details), `cost` (with `Statistics(sizeInBytes=…, rowCount=…)`) and `codegen`.
- **Spark 3.x and 4.x** plans, including **AQE** (`AdaptiveSparkPlan isFinalPlan=true/false`, initial vs. final plan, query stages, `AQEShuffleRead` coalesced/skewed reads), whole-stage-codegen markers `*(n)`, `ReusedExchange` and `ReusedSubquery`, scalar subqueries and dynamic partition pruning, `Union`, `Expand`, `Window`, Python / pandas UDF operators, `InMemoryTableScan`, write commands, and **Photon** operator names.
- **Event logs** (the JSON-lines file from `spark.eventLog.dir`, uncompressed): every SQL execution with its final AQE plan, plus SQL metrics such as rows, data size, files read, spill and shuffle records, including the per-task spread (min / median / max) used to measure skew. The JSON from the Spark UI REST endpoint `/api/v1/applications/<app>/sql` is accepted too.
- Pasted text with table borders (`| … |`), escaped `\n` from a notebook or JSON string, missing headers, and truncated plans.

## What it shows

- **Findings** with severity, a plain-English explanation, why it matters, and fixes as PySpark, SQL hint and config, each with a copy button (and "Copy all fixes"). There are 20 rules: sort-merge joins that could be broadcast (with sizes from statistics or metrics), nested-loop and cartesian joins, measured skew and AQE skew handling, missing partition pruning (including UDFs on the partition column), filters that are not pushed down and why, Python UDFs, wide read schemas, repeated scans, windows without `PARTITION BY`, global sorts, `count(DISTINCT)` Expand, `coalesce(1)` / `repartition(1)`, AQE turned off, `collect_list` + `explode`, CSV/JSON scans, small files, spill, very large broadcasts, and Photon fallbacks. There is also a health score from 0 to 100.
- **Tree view**: a collapsible operator tree with codegen badges, stage numbers and a metrics overlay. Arrow keys move, expand and collapse it.
- **Graph view**: a pan and zoom DAG (drag, wheel, pinch, or `+` `−` `0` and the arrow keys) with stage boundaries, codegen colors, reuse links, flagged operators and a metrics overlay (rows, size, spill, skew). Adapter "plumbing" nodes can be hidden.
- **Search** across operators, tables and columns, with match highlighting and Next / Previous.
- **Operator details**: a glossary note, attributes, full metrics with the per-task spread, related findings and the raw plan line. Operator names show glossary tooltips on hover and focus.
- **Compare** two plans (before and after a fix): operator count changes, join strategy changes, scan changes (pruning, pushed filters, columns, format), shuffles, resolved and new findings, and a line diff of the plan tree that ignores expression ids.
- **Samples**: 29 plans plus 8 before/after fix pairs, generated with PySpark 4.0.1 and 3.5.7 (the Photon sample is a hand-written illustration).
- **Share links**: the input is compressed into the URL hash, so it never reaches a server. Very large event logs are too big for a link, so export those instead.
- **History** of your recent plans in `localStorage`, with rename, delete (with undo) and clear (with undo).
- **Export** to Markdown, standalone HTML or JSON.
- Light and dark themes, responsive layout, keyboard shortcuts (`?` lists them), and installable as a PWA.

## Getting a plan

```python
df.explain(mode="formatted")   # or "simple", "extended", "cost", "codegen"
```

```sql
EXPLAIN FORMATTED SELECT ...;
EXPLAIN COST SELECT ...;   -- adds size estimates, which make the broadcast advice concrete
```

For metrics, run the job with `spark.eventLog.enabled=true` and open the file from `spark.eventLog.dir`. If it is compressed, decompress it first, or set `spark.eventLog.compress=false`.

## Privacy

Everything runs in the page. There are no servers, analytics or third-party requests. History and settings stay in your browser's `localStorage`. Share links keep the plan after `#`, which browsers do not send to servers, but anyone who has the link can read the plan, including table and column names.

## Limits

- Without statistics (`EXPLAIN COST`, a final AQE plan) or event-log metrics, data sizes are unknown, so size-based advice is hedged.
- Event-log metrics are mapped onto operators on a best-effort basis. REST `/sql` output is matched to operators by name.
- The Photon sample is a hand-written illustration, not captured from Databricks.
- Some plans keep a filter unpushed for legitimate reasons. For example, `CASE WHEN … <=> true` from a `when()` expression still shows a pushdown finding with the specific blocker.
- Other engines (Trino, Hive on Tez, Databricks SQL's query profile JSON) are not supported.

## Development

No build step: it is plain HTML, CSS and JavaScript with no dependencies.

```bash
python3 -m http.server 8080    # then open http://localhost:8080
npm test                       # 63 Node tests (node --test), Node 18+
```

The parser, event-log reader, diagnostics, diff, layout and reports are plain modules in `js/` that also load in Node, so the tests use the real samples.

## License

MIT
