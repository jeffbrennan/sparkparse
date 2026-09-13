# sparkparse — Project Context

This file gives Claude Code sessions quick orientation on the codebase architecture,
key conventions, and current development state.

## What this project does

sparkparse parses Apache Spark event logs (JSONL format, produced when
`spark.eventLog.enabled=true`) into structured Polars DataFrames and renders an interactive
Dash dashboard for identifying performance bottlenecks.

## Architecture

```
Spark event log (JSONL)
        │
        ▼
  sparkparse/eventlog.py       – discover logical log sources (rolled, compressed,
        │                         in-progress) and stream their events line by line
        ▼
  sparkparse/parse.py          – parse raw log into ParsedLog (Pydantic model)
        │
        ▼
  sparkparse/clean.py          – transform ParsedLog into two Polars DataFrames:
        │                         .dag      – one row per physical plan node per query
        │                         .combined – one row per task with full job/stage/query context
        ▼
  sparkparse/models.py         – all Pydantic models and the NodeType / NodeTypeDetailMap
        │
        ├──▶ sparkparse/metrics.py   – canonical metric registry (names, units, scopes)
        ├──▶ sparkparse/analyze.py   – raw plan export + evidence-based findings
        ├──▶ sparkparse/app.py       – Typer CLI (get, viz commands)
        ├──▶ sparkparse/dashboard.py – Dash app (multi-page: home, summary, dag)
        └──▶ sparkparse/capture.py   – context manager / decorator for live Spark sessions
```

## Key files

| File                    | Purpose                                                                                                                                                          |
| ----------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `sparkparse/models.py`  | All Pydantic models. `NodeType` enum (30+ values), `ParsedLog`, `ParsedLogDataFrames`, `NODE_TYPE_DETAIL_MAP` (node type → detail model class)                   |
| `sparkparse/parse.py`   | `get_parsed_metrics()` is the main entry point (`get_all_parsed_metrics()` for every application in a directory). `parse_source()` parses one logical log incrementally; `parse_spark_ui_tree()` converts indented ASCII plans to node graphs.     |
| `sparkparse/eventlog.py`| Event-log source discovery and streaming. `discover_sources()` collapses rolled `eventlog_v2_*` directories into one source and skips markers/checksums; `resolve_source()` implements explicit selection and the newest-source default; `iter_lines()` streams segments. Readable codecs: none, `zstd`, `gz`. |
| `sparkparse/schemas.py` | Canonical typed empty frame schemas (`DAG_SCHEMA`, `COMBINED_SCHEMA`) shared by the event-log and Connect paths. |
| `sparkparse/clean.py`   | `log_to_dag_df()` and `log_to_combined_df()` produce the two output DataFrames. `get_readable_size()` and `get_readable_timing()` are Polars expression helpers. |
| `sparkparse/app.py`     | Typer CLI. `get` → parses and writes output files. `viz` → launches dashboard.                                                                                   |
| `sparkparse/connect.py` | Spark Connect adapter. Intercepts client action boundaries and `_build_metrics`, attributes metrics per execution/thread, and builds the dag frame. `probe_connect_support()` reports the client surface; `SparkConnectCapture.from_plan_metrics()` replays recorded executions offline. |
| `sparkparse/metrics.py` | Canonical metric registry. Maps verified classic and Photon/Connect raw metric names onto canonical names with unit, scope and aggregation. Unrecognized metrics are preserved with `canonical=None`, never guessed. |
| `sparkparse/analyze.py` | `to_plan_summary()` exports raw plan facts; `analyze_dfs()` runs the diagnostic rules and returns an `AnalysisReport` (findings + per-rule `evaluated`/`unsupported`/`insufficient_data` status). `find_*()` helpers are the DataFrame interface. |
| `sparkparse/capture.py` | `SparkparseCapture` context manager/decorator. Borrows configured sessions, supports explicit owned sessions, and finalizes results on `__exit__`.              |

## Data model

### `dag` DataFrame columns (per physical plan node per query)

- `query_id`, `query_function`, `query_header`, `query_start/end_timestamp`, `query_duration_seconds`
- `node_id`, `node_type`, `node_name`, `child_nodes` (comma-separated string), `whole_stage_codegen_id`
- `details` — JSON string; deserialize with the appropriate model from `NODE_TYPE_DETAIL_MAP`
- `accumulator_totals` — list of structs: `{metric_name, metric_type, value, value_exact, readable_str, unit}`. `value` is `Float64`; `value_exact` is the unrounded `Int64` for metrics that were not rescaled (null for `nsTiming` and `average`), so counts above 2**53 survive.
- `node_duration_minutes`

### `combined` DataFrame columns (per task)

- `log_name`, `parsed_log_name`, `query_id`, `query_function`, `query_count`
- `query/job/stage/task` start/end timestamps and duration_seconds
- `stage_attempt_id`, `stage_num_tasks`, `stage_status`, `stage_failure_reason`
- `task_id`, `partition_id`, `attempt`, `task_status`, `task_succeeded`,
  `task_failure_reason`, `executor_id`, `nodes` (plan nodes this task fed)
- Executor metrics: `executor_run_time_seconds`, `executor_cpu_time_seconds`, `jvm_gc_time_seconds`, `peak_execution_memory_bytes`
- Input/output: `bytes_read`, `records_read`, `bytes_written`, `records_written`
- Shuffle: `shuffle_remote_bytes_read`, `shuffle_local_bytes_read`, `shuffle_bytes_written`
- Spill: `memory_bytes_spilled`, `disk_bytes_spilled`

### Attempt and association contract

- One row per task **attempt**, keyed by `(stage_id, stage_attempt_id, task_id)`.
  A retried stage keeps both attempts; keying on `stage_id` alone would fold a
  retry's metrics into the original.
- Resource usage (`executor_run_time_seconds`, spill, GC) counts every attempt;
  output accounting (`bytes_read/written`, `records_*`, shuffle bytes) counts
  only *retained* outputs. Success alone is not enough: a losing speculative
  copy and a partition recomputed in a later stage attempt both end
  successfully, so `analyze.retained_outputs()` keeps one attempt per
  `(stage_id, partition)` — latest stage attempt, earliest finish within it.
  `to_plan_summary()["total_basis"]` records which basis each total used.
- A stage can belong to several jobs and serve several queries. Those relations
  live in `ParsedLogDataFrames.job_stage` / `.query_stage`; joining them into
  `combined` would duplicate task rows and double-count their metrics. The task
  frame carries the earliest attributed query plus `query_count`.
- Tasks no SQL execution claims (schema inference, RDD work) stay in `combined`
  with a null `query_id` rather than being dropped.

### Event-log source contract

- `parse_source()` requires neither `SparkListenerApplicationStart` nor an
  adaptive execution update. The plan comes from the strongest event seen:
  final adaptive plan > in-progress adaptive plan > the plan on
  `SQLExecutionStart`. A non-AQE workload is normal, not an error.
- A line that fails to decode is `corrupt_line` when more lines follow and
  `truncated_tail` when it is the last line of the last segment. Tolerant mode
  records both in `ParsedLog.diagnostics`; `strict=True` raises with URI and
  line number.
- Missing data stays missing: a task with no `Task Metrics` has `metrics=None`,
  a query with no end event has a null end timestamp and duration.
- Reads are incremental: segments stream line by line and the log text is never
  copied whole (`tests/test_eventlog.py` asserts no `read()`/`readlines()` on a
  log handle). Peak RSS still grows with retained model state — roughly 36 KB
  per task on the recorded fixtures. `python -m tests.benchmark_ingestion
  <log_dir> [log_file]` reports elapsed time and peak RSS for a single log.

### Metric and findings contract

- Raw operator metrics are normalized through `sparkparse/metrics.py`. Use
  `canonical_metrics(acc_totals, source)` or `metric_value(acc_totals, "output_rows")`
  rather than matching raw metric names: `number of output rows` (classic) and
  `numOutputRows` (Photon/Connect) are the same canonical metric.
- A missing metric is `None`; a measured zero is `0`. Never use `or` to default a
  metric value — it destroys a legitimate zero. The same applies to task columns:
  summing a null column yields zero, which reads as "measured zero". Check for
  usable values (`measured_stages()`) before a rule claims it evaluated anything.
- Integer counts stay exact end to end: both ingestion paths write `value_exact`
  alongside the `Float64` `value`, and normalization reads it and never routes an
  integer through `float`. When adding a metric path, populate `value_exact` for
  any metric you do not rescale.
- `scanned_rows` ≠ `output_rows`, operator `spill_bytes` ≠ task memory/disk spill,
  and `data size` is not scan bytes. Aliases are only registered when verified.
- Analysis rules return a `RuleAssessment` even when they produce no findings, so a
  silent rule (`unsupported`, `insufficient_data`) is distinguishable from a clean
  result. `get_coverage_notes()` surfaces those to the dashboard and CLI.

### `details` column deserialization

The `details` column in `dag` is a JSON string. To access structured data:

```python
import json
from sparkparse.models import NODE_TYPE_DETAIL_MAP, NodeType

row = dag.filter(pl.col("node_type") == "Scan").to_dicts()[0]
detail_cls = NODE_TYPE_DETAIL_MAP[NodeType.Scan]
detail = detail_cls.model_validate(json.loads(row["details"])["detail"])
# detail.location.location → list of file paths
```

`deserialize_scan_detail(details_str)` in `sparkparse/parse.py` is a convenience wrapper for Scan nodes.

## Testing

```bash
# fast unit tests (no Spark needed)
just ci

# full integration tests (requires a local JVM + PySpark)
just ci-full
```

Test fixtures live in `tests/data/`:

- `tests/data/full_logs/` — three real Spark event logs for end-to-end testing
- `tests/data/test_*_parsing/` — extracted JSON/TXT fragments for unit testing specific parsing steps

## Development commands

```bash
uv sync --dev                          # install including dev deps
uv run ruff check sparkparse/ tests/   # lint
uv run ruff format sparkparse/ tests/  # format
uv run pyrefly check sparkparse/ tests/  # type check
```

## Planned improvements (see IMPLEMENTATION.md for full detail)

### PR 1 — Tooling modernization

- Move dev deps (`pytest`, `ruff`, `ipykernel`) to `[tool.uv.dev-dependencies]`
- Add `pyrefly` for type checking
- Add `[tool.ruff]` config to `pyproject.toml`
- Add `.github/workflows/test.yml` CI workflow

### PR 2 — Unit test expansion

- Add `tests/test_clean.py` covering `get_readable_size`, `get_readable_timing`, `clean_jobs`,
  `clean_stages`, `clean_tasks`, `get_job_idle_time`

### PR 3 — LLM-friendly analysis output

- Add `sparkparse/analyze.py` with two distinct concerns:
    - `to_plan_summary(dfs, log_name) -> dict` — token-efficient structured plan data for LLM
      piping; presents raw facts (nodes, durations, bytes, join types, paths) without pre-assigned
      severity or pre-classified findings; the LLM draws its own conclusions. The value over
      `df.explain()` is runtime metrics (per-node durations, scan bytes/records) correlated from
      accumulator updates — plan structure alone is not worth re-encoding.
    - `find_*()` helpers — programmatic analysis functions for interactive/notebook use
      (`find_cartesian_joins`, `find_largest_scans`, `find_repeated_scans`, `find_spill`, etc.)
- Add `tests/test_analyze.py`

### PR 4 — CLI improvements

- Add `sparkparse analyze` command (prints JSON findings, pipeable to LLM)
- Improve help strings on existing commands
- Add `"analyze"` action to `capture.py`
- Replace `print()` with `logging` in `capture.py`

### PR 7 — Complete Pydantic coverage

- Add `NodeType.Unknown` sentinel and `RawDetail` fallback model so unrecognized node types
  degrade gracefully instead of raising (currently `parse_spark_ui_tree` and `get_plan_details`
  both hard-fail on unknown types — see `parse.py:278` and `parse.py:145`)
- Wrap `NodeType(...)` and `QueryFunction(...)` calls in try/except with warning logs
- Add detail models for the standard Spark 3.5 operator set not yet covered:
    - Aggregate variants: `ObjectHashAggregate`, `SortAggregate`, `Expand`
    - Python/Pandas UDF nodes: `ArrowEvalPython`, `BatchEvalPython`, `MapInPandas`,
      `MapInArrow`, `FlatMapGroupsInPandas`, `FlatMapCoGroupsInPandas`
    - DataSource V2: `BatchScanExec`, `WriteToDatasourceV2`, `AppendData`,
      `OverwriteByExpression`, `OverwritePartitionsDynamic`
    - Subquery: `SubqueryExec`, `ReusedSubqueryExec`, `SubqueryBroadcast`
    - Misc: `RepartitionByExpression`, `Sample`, `Range`, `CartesianProduct`
- Extend `ExchangeType` with `REPARTITION_BY_COL`, `REPARTITION_BY_NUM`, `REPARTITION`
- Extend `QueryFunction` with `show`, `collect`, `first`, `head`, `take`, etc.
- Databricks/Photon-specific nodes handled by `Unknown` fallback until real fixtures available
- Add test fixtures and cases for each new node type in `tests/test_detail_parsing.py`

### PR 5 — Cloud storage support

- Add `sparkparse/storage.py` with path-agnostic I/O (`open_file`, `write_text`, `list_files`,
  `copy_file`, `remove_dir`) backed by `fsspec` for cloud URIs and stdlib for local paths
- Update `parse.py`, `common.py`, `capture.py` to route all file I/O through `storage.py`
- Accept cloud URIs (`s3://`, `abfss://`, `gs://`) as `temp_dir` in `capture.py`; Spark writes
  event logs there natively, sparkparse reads them back via fsspec
- Add optional `[s3]`, `[azure]`, `[gcs]`, `[cloud]` extras to `pyproject.toml`
- Target: Databricks and other ephemeral cluster environments where local disk is gone at
  cluster termination

### PR 6 — Performance history and alerts

- Add `sparkparse/history.py` — append-only run history: `record_from_dfs()` derives a compact
  `RunRecord` (duration, bytes, spill, shuffle, cartesian join count, etc.) from
  `ParsedLogDataFrames`; `append()` writes to Delta (primary) or JSONL (fallback); `read()`
  queries history filtered by `log_name`
- Add `sparkparse/alerts.py` — `AlertConfig` model, `load_alert_config()` from TOML,
  `check_alerts()` evaluates rules (pct_increase, absolute_increase, threshold) against history
  and dispatches via `on_trigger` (`"log"`, `"raise"`, `"file"`)
- Add `RunRecord` Pydantic model to `sparkparse/models.py`
- Extend `capture.py` with `history_path`, `log_name`, `alert_config` params — history append
  and alert check run automatically in `__exit__`
- Add `sparkparse history` and `sparkparse check-alerts` CLI commands
- Alert config defined in TOML; cloud paths supported via PR 5's `storage.py`

## Known quirks

- Spark's `lz4`, `lzf` and `snappy` event-log codecs use Java-specific block
  framing that no Python codec reads; those raise `UnsupportedCodecError` naming
  the codec. `zstd` needs the `zstandard` package (`sparkparse[zstd]`).
- `capture.py` borrows supplied SparkSessions and requires event logging to be enabled
  before capture. Use `cap.spark` inside the context; opt into an owned session explicitly.
- `test.py` and `test_capture.py` in `tests/` are integration tests that spin up a local
  SparkSession. They are slow and cannot run in CI without a JVM available.
- `falsa` is listed in `[project.dependencies]` but its usage in the codebase is unclear —
  verify before removing.
- Cloud path support (`s3://`, `abfss://`, `gs://`) requires the relevant fsspec backend
  (`s3fs`, `adlfs`, `gcsfs`) to be installed and cloud credentials to be configured.
  On Databricks, DBFS paths (`/dbfs/...`) and Unity Catalog volumes (`/Volumes/...`) mount
  as local filesystem paths and work without any additional configuration.
- The `details` column stores Pydantic model data as a JSON string (not a Polars struct)
  to keep the schema flexible across 25+ node types.
