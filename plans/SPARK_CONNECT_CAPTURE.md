# SparkConnectCapture — Implementation Brief

## Goal

Add `sparkparse/connect.py` with a `SparkConnectCapture` context manager that captures
Spark query execution metrics on Databricks serverless (Spark Connect) without stopping
or restarting the session. Output must be a `ParsedLogDataFrames` with the same schema
as event-log-based parsing so all downstream functions (`plot_dag`, `get_issues`,
`to_plan_summary`, etc.) work without modification.

---

## Why this is needed

`SparkparseCapture` (in `sparkparse/capture.py`) works by stopping and restarting the
SparkSession to inject `spark.eventLog.enabled=true`. On Databricks serverless:

- `spark.conf.get("spark.eventLog.dir")` throws even with a `None` default (blocked by
  the serverless runtime — `client.5.7` via gRPC)
- `spark.sparkContext` is not accessible (Spark Connect has no JVM surface)
- `spark.stop()` cannot be called (the session is managed by Databricks)

The runtime is `DATABRICKS_RUNTIME_VERSION=client.5.7`. The Python notebook process is
a thin client connected to a remote Spark driver via Unix socket
(`SPARK_REMOTE=unix:///databricks/sparkconnect/grpc.sock`). All driver-side data
(event logs, Spark UI on 4040, task-level metrics) is on a separate pod that is
inaccessible from the notebook environment.

Detection: `spark.sparkContext` raises on serverless, succeeds on classic clusters.

---

## How the data is captured

`spark._client` is a `pyspark.sql.connect.client.core.SparkConnectClient`. Every time a
query is executed (`.collect()`, `.show()`, `.count()`, etc.) the gRPC response includes
execution metrics. PySpark processes these in `_build_metrics`:

```python
# signature (confirmed on Databricks serverless)
spark._client._build_metrics(
    metrics: Sequence[pb2.ExecutePlanResponse.Metrics]
) -> Sequence[PlanMetrics]
```

Monkey-patching this method intercepts the metrics for every query run inside the
context block:

```python
orig = spark._client._build_metrics
captured = []

def patched(metrics_proto):
    result = orig(metrics_proto)
    captured.extend(result)
    return result

spark._client._build_metrics = patched
# ... user queries run here ...
spark._client._build_metrics = orig
```

This was tested and confirmed to work. 6 nodes were captured for a simple groupBy query.

---

## PlanMetrics data model

`pyspark.sql.metrics.PlanMetrics` — confirmed fields:

| Field | Type | Notes |
|---|---|---|
| `plan_id` | int | Node ID (large int, e.g. 7504) |
| `parent_plan_id` | int | Parent node ID; root node has `parent_plan_id == plan_id` |
| `name` | str | Full node name, e.g. `"PhotonScan parquet samples.nyctaxi.trips"` |
| `metrics` | list[MetricValue] | Per-node execution metrics |

`MetricValue` fields (accessed as `m.name`, `m.value`, `m.metric_type`):

| Field | Example | Notes |
|---|---|---|
| `name` | `"cumulTime"` | Metric name |
| `value` | `3430351` | Raw numeric value |
| `metric_type` | `"nsTiming"` | See type table below |

### MetricValue types and unit conversions

| `metric_type` | Unit | Conversion for `readable_value`/`readable_unit` |
|---|---|---|
| `nsTiming` | nanoseconds | Use `get_readable_timing(ns / 1_000_000)` (convert to ms first) |
| `timing` | milliseconds | Use `get_readable_timing(value)` directly |
| `size` | bytes | Use `get_readable_size(value)` |
| `sum` | raw count | No conversion, `readable_unit = ""` |
| `count` | raw count | No conversion, `readable_unit = ""` |
| `average` | raw | No conversion, `readable_unit = ""` |
| `minimum` | raw | No conversion, `readable_unit = ""` |
| `maximum` | raw | No conversion, `readable_unit = ""` |

**Important:** `get_readable_size` and `get_readable_timing` in `sparkparse/clean.py`
work on Polars expressions, not scalars. Replicate their breakpoints directly:

```python
def _readable_size(bytes_val):
    if bytes_val is None: return None, ""
    for thresh, unit in [(1<<40,"TiB"),(1<<30,"GiB"),(1<<20,"MiB"),(1<<10,"KiB")]:
        if bytes_val >= thresh:
            return round(bytes_val / thresh, 1), unit
    return bytes_val, "B"

def _readable_timing(ms_val):
    if ms_val is None: return None, ""
    if ms_val >= 3_600_000: return round(ms_val / 3_600_000, 1), "hr"
    if ms_val >= 60_000: return round(ms_val / 60_000, 1), "min"
    if ms_val >= 1_000: return round(ms_val / 1_000, 1), "s"
    return round(ms_val, 1), "ms"
```

### Key metric names to extract for top-level dag columns

| dag column | Source metric | Notes |
|---|---|---|
| `node_duration_minutes` | `cumulTime` (nsTiming) | `value / 60_000_000_000` |
| spill detection | `numBytesSpilled` (size) | used by `find_spill` |
| scan rows | `numRowsScanned` (sum) | on PhotonScan nodes |
| output rows | `numOutputRows` (sum) | all nodes |
| peak memory | `peakMemoryUsage` (size) | all nodes |

### maximum/minimum metrics are NOT cross-task

Confirmed through testing with `repartition(8)`: `maximum`/`minimum` metrics are Photon
engine internals (hash table stats, var-len key sizes, pivot batch sizes), not cross-task
aggregates. **There is no path to per-task data through Spark Connect.**

---

## DAG structure reconstruction

`parent_plan_id` fully describes the tree. The root node has `parent_plan_id == plan_id`.

Build `child_nodes` (comma-separated string, matching event-log schema) by inverting:

```python
from collections import defaultdict
children = defaultdict(list)
for node in plan_metrics:
    if node.parent_plan_id != node.plan_id:  # skip root
        children[node.parent_plan_id].append(node.plan_id)

child_nodes_map = {nid: ", ".join(str(c) for c in sorted(cs))
                   for nid, cs in children.items()}
```

---

## Node type mapping

Node names from Spark Connect are Photon-prefixed. Map to existing `NodeType` enum
where semantically equivalent; use `NodeType.Unknown` for anything else. The
`NodeType.Unknown` sentinel + `RawDetail` fallback already exists (PR 7 in CLAUDE.md).

| PlanMetrics `name` prefix | NodeType mapping |
|---|---|
| `PhotonScan` | `NodeType.Scan` |
| `PhotonGroupingAgg` | `NodeType.HashAggregate` |
| `PhotonSort` | `NodeType.Sort` |
| `PhotonShuffleExchangeSink` / `Source` | `NodeType.Exchange` |
| `PhotonShuffleMapStage` / `ResultQueryStage` | `NodeType.Unknown` |
| `PhotonColumnarToRow` | `NodeType.Unknown` |
| `AdaptiveSparkPlan` | `NodeType.AdaptiveSparkPlan` |
| anything else | `NodeType.Unknown` |

Extract the prefix by splitting on the first space: `name.split()[0].replace("Photon", "")`.

---

## Schema requirements for dag DataFrame

The `dag` DataFrame must match `log_to_dag_df` output exactly so `plot_dag`,
`get_issues`, `to_plan_summary`, etc. work unchanged. Required columns (all nullable
unless noted):

```
log_name                str
parsed_log_name         str
query_id                Int64  (not nullable)
query_function          str
query_header            str
query_start_timestamp   str
query_end_timestamp     str
query_duration_seconds  Float64
node_id                 Int64  (not nullable)
node_type               str
node_name               str    (format: "[node_id_adj] NodeType")
child_nodes             str    (comma-separated, nullable if leaf)
whole_stage_codegen_id  Int64  (nullable — set null for Photon nodes)
details                 str    (JSON string, use '{"detail": null}' for Connect nodes)
accumulator_totals      List[Struct]  (see struct fields below)
n_accumulator_totals    Int64
node_duration_minutes   Float64 (nullable)
n_accumulators          Int64
node_id_adj             Int64  (same as node_id for Connect — no codegen offset)
```

`accumulator_totals` struct fields (must match `clean.py` schema exactly):
```
metric_name    str
metric_type    str
value          Float64
readable_value Float64
readable_unit  str
readable_str   str    (e.g. "3.5 ms", "2.8 GiB")
```

---

## Schema requirements for combined DataFrame

Return an empty DataFrame with the correct schema. All downstream functions check for
empty before operating. Required columns (subset that matters for type compatibility):

```
log_name, parsed_log_name, query_id (Int64), stage_id (Int64), task_id (Int64),
task_duration_seconds (Float64), bytes_read (Int64), records_read (Int64),
shuffle_bytes_read (Int64), memory_bytes_spilled (Int64), disk_bytes_spilled (Int64),
executor_id (str), nodes (List[str])
```

Use `pl.DataFrame(schema={...})` with an empty dict to create a zero-row DataFrame with
the right schema.

---

## Multiple queries in one context block

Each time `_build_metrics` is called with a non-empty result it corresponds to one query
execution. Track calls and increment `query_id` per call. Assign `query_header` from the
root node name (the `AdaptiveSparkPlan` node's child description or just `f"query_{id}"`).

---

## What works vs what doesn't on serverless

**Works** (plan-level metrics sufficient):
- `plot_dag(dfs)` — uses `dag.accumulator_totals` and `dag.node_duration_minutes`
- `find_spill(dfs)` — uses `dag` `numBytesSpilled` metric
- `find_largest_scans(dfs)` — uses `dag` `numRowsScanned` metric
- `to_plan_summary(dfs)` — uses `dag`
- `get_issues(dfs)` — runs all finders, empty results for task-level ones is fine

**Returns empty** (requires task-level data not available via Spark Connect):
- `find_skewed_tasks(dfs)`
- `find_shuffle_heavy_stages(dfs)`

---

## Files to create / modify

### New: `sparkparse/connect.py`

```python
class SparkConnectCapture:
    def __init__(self, spark, log_name=None): ...
    def __enter__(self): ...     # patch _build_metrics
    def __exit__(self, ...): ... # restore, call _build_dataframes
    def _build_dataframes(self) -> ParsedLogDataFrames: ...
    def _build_dag_df(self) -> pl.DataFrame: ...
    def _build_combined_df(self) -> pl.DataFrame: ...  # empty, correct schema
    
    @property
    def dfs(self) -> ParsedLogDataFrames | None: ...
```

### Modify: `notebooks/sparkparse_demo.py`

Replace the NYC taxi capture section to auto-detect classic vs serverless:

```python
try:
    spark.sparkContext  # raises on serverless
    from sparkparse.capture import SparkparseCapture as _Cap
except Exception:
    from sparkparse.connect import SparkConnectCapture as _Cap

with _Cap(action="analyze", spark=spark, ...) as cap:
    ...
```

Note: `SparkConnectCapture` does not take an `action` parameter (always captures and
analyzes). The `log_name` param is the only one that matters.

---

## Testing

```bash
# Unit tests — run after implementation, all 219 should still pass
just ci

# Local smoke test (no Spark Connect, just schema validation)
uv run python -c "
from sparkparse.connect import SparkConnectCapture
# Can't fully test without a Databricks session, but at minimum:
# 1. Import succeeds
# 2. _build_dag_df with mock PlanMetrics produces correct schema
"
```

On Databricks serverless: run the nyctaxi capture section in
`notebooks/sparkparse_demo.py` and verify `plot_dag(dfs)` renders, `get_issues(dfs)`
returns results, `dfs.combined.is_empty()` is True.

---

## Reference: explored and ruled out

- **Event log via session restart** — `spark.conf.get("spark.eventLog.dir")` throws even
  with None default on serverless; `spark.stop()` not allowed
- **Spark UI driver proxy** (`/driver-proxy/o/{org}/{cluster}/4040/...`) — returns 500
  TEMPORARILY_UNAVAILABLE for REPL/serverless cluster type
- **`spark.sparkContext._conf.getAll()`** — not available on Spark Connect
- **Dynamic `spark.conf.set("spark.eventLog.enabled", "true")`** — config not available
- **System tables** (`system.compute.query_history`, etc.) — user rejected; not
  universally accessible across workspaces
- **cross-task max/min metrics** — confirmed these are Photon engine internals, not
  cross-task aggregates
