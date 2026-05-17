# sparkparse Implementation Roadmap

This document describes the planned improvements to sparkparse across three high-level goals,
broken into self-contained PRs that can be reviewed and merged independently.

## Goals

1. **Modernize** — UV dev groups, ruff config, pyrefly type checking, more unit tests, CI
2. **Better UX** — improved CLI, cleaner SparkSession wrapper, richer output
3. **LLM-friendly analysis** — token-efficient structured plan summary for piping to an LLM
   (queries, nodes, durations, bytes, join types, paths — raw facts, no pre-assigned severity),
   plus programmatic `find_*` helpers for interactive analysis in notebooks/scripts
4. **Cloud storage** — read logs from and write output to blob storage (S3, ADLS, GCS) for
   ephemeral cluster environments like Databricks
5. **Performance history and alerts** — append-only run history for trending metrics over time,
   with configurable regression alerts that trigger when a job degrades beyond a threshold

---

## PR 1: Tooling Modernization

**Branch:** `feat/tooling-modernization`

### Changes

**`pyproject.toml`**
- Move `pytest`, `ruff`, `ipykernel` out of `[project.dependencies]` into `[tool.uv.dev-dependencies]`
  so they are not included in the published wheel
- Add `pyrefly` to dev dependencies (Meta's type checker, replaces pyright)
- Add `[tool.ruff]` config block with sensible defaults for this codebase:
  - `line-length = 100`, `target-version = "py311"`
  - Rules: `E`, `F`, `I` (isort), `UP` (pyupgrade), `B` (bugbear), `C4`, `RUF`
  - Ignore `B008` (Typer's default-argument pattern triggers this)
- Add `[tool.pytest.ini_options]` — `testpaths = ["tests"]`, exclude `test.py` and `test_capture.py`
  (both require a live Spark session and cannot run in CI without heavy setup)
- Update description from placeholder to actual description
- Add `[tool.pyrefly]` config block

**`.github/workflows/test.yml`** (new)
- Trigger on push to any branch and on PRs
- Steps: checkout → setup Python 3.11 → install uv → `uv sync --dev` → ruff check → ruff format
  check → pytest (excluding Spark-dependent tests)

### Verification

```bash
uv sync --dev
uv run ruff check sparkparse/ tests/
uv run ruff format --check sparkparse/ tests/
uv run pytest tests/ --ignore=tests/test.py --ignore=tests/test_capture.py -v
```

---

## PR 2: Unit Test Expansion

**Branch:** `feat/expand-tests`

### Changes

**`tests/test_clean.py`** (new)

Target functions in `sparkparse/clean.py` that have no coverage. All tests use small inline
Polars DataFrames or Pydantic model instances — no Spark session required.

- `get_readable_size()` — test byte, KiB, MiB, GiB, TiB boundaries using `pl.lit()` in a select
- `get_readable_timing()` — test ms, s, min, hr thresholds
- `get_readable_col()` — test the facade function for both size and timing types
- `clean_jobs()` — construct two `Job` instances (start + end), assert `job_duration_seconds` and
  the stage_id explode
- `clean_stages()` — construct two `Stage` instances, assert `stage_duration_seconds`
- `clean_tasks()` — construct a `Task` instance, assert `task_duration_seconds`
- `get_job_idle_time()` — construct a minimal combined DataFrame with two jobs separated by a gap,
  assert the idle time is detected

Test pattern for Polars expression functions:

```python
def _eval_size(value: int) -> dict:
    df = pl.DataFrame({"v": [value]})
    return df.select(get_readable_size(pl.col("v"))).unnest("v").to_dicts()[0]
```

### Verification

```bash
uv run pytest tests/test_clean.py -v
```

---

## PR 3: LLM-Friendly Analysis Output

**Branch:** `feat/analyze-module`

### Design

`analyze.py` has two separate concerns:

1. **`to_plan_summary(dfs, log_name) -> dict`** — token-efficient output for LLM piping, used
   by the CLI. The plan structure alone (node types, parent/child relationships) is already
   available from `df.explain()` and isn't meaningfully better when re-encoded as JSON. The
   value here is the **runtime metrics correlated to plan nodes**: actual measured durations,
   bytes scanned, and rows produced from accumulator updates that Spark never exposes in a
   single queryable surface. Without these metrics, `to_plan_summary` is just a lossy
   re-encoding of `explain()`.

2. **`find_*()` helpers** — programmatic analysis functions for interactive/notebook use.
   These return structured results but are not included in the piped JSON output.

### What makes the plan summary worth producing

- **Per-node runtime durations** — sparkparse cross-references accumulator updates (spread
  across `SparkListenerSQLAdaptiveExecutionUpdate` and `SparkListenerTaskEnd` events) back to
  specific plan nodes. This is not available from `explain()` or any single Spark API.
- **Scan metrics on the node** — bytes read and record counts from accumulator totals, placed
  directly on the Scan node they belong to rather than scattered across task events.
- **Cross-query view** — a single event log contains many queries. Identifying that the same
  path is scanned repeatedly across queries requires joining across events; sparkparse does
  this join already.
- **Task-level aggregates** — spill and shuffle totals live in `SparkListenerTaskEnd` events
  (potentially thousands per log). The summary aggregates these per stage so the LLM sees
  one row per stage rather than one row per task.
- **Token efficiency** — a real event log is typically 1–10 MB of JSONL. The plan summary
  for the same log is ~10–50 KB.

### Plan summary output format

Short field names; omit null/zero fields. Runtime metrics are the primary content —
plan structure fields (join type, keys, order) are included as supporting context only.

```json
{
  "log": "my_job",
  "duration_s": 272.4,
  "bytes_read": 3045000000,
  "bytes_written": 0,
  "shuffle_bytes": 1200000000,
  "spill_bytes": 0,
  "queries": [
    {
      "id": 1,
      "fn": "count",
      "duration_s": 45.2,
      "nodes": [
        {"id": 4, "type": "Scan", "duration_min": 0.4, "bytes": 1073741824, "records": 12000000, "paths": ["/data/warehouse/orders"]},
        {"id": 3, "type": "Filter", "duration_min": 0.1, "records_out": 8400000},
        {"id": 2, "type": "BroadcastNestedLoopJoin", "duration_min": 2.1, "join_type": "Cross"},
        {"id": 1, "type": "HashAggregate", "duration_min": 0.3, "records_out": 94200},
        {"id": 0, "type": "Sort", "duration_min": 0.6}
      ]
    }
  ],
  "stages": [
    {"id": 0, "duration_s": 12.1, "tasks": 200, "spill_bytes": 0, "shuffle_read_bytes": 0, "shuffle_write_bytes": 104857600}
  ]
}
```

Fields per node (all sourced from accumulator totals — omit if absent):
- All nodes: `duration_min`, `records_out` (number of output rows)
- `Scan`: `bytes` (size of files read), `records` (number of output rows), `paths`
- `BroadcastNestedLoopJoin`, `SortMergeJoin`, `BroadcastHashJoin`: `join_type`, `join_condition`
- `Exchange`, `AQEShuffleRead`: `shuffle_write_bytes`, `shuffle_read_bytes`

### `find_*` programmatic helpers

These are available via `from sparkparse.analyze import find_cartesian_joins` etc. They are
**not** called by `to_plan_summary` — they exist for interactive analysis in notebooks or
scripts where the user wants structured programmatic output.

| Function | Source | Returns |
|---|---|---|
| `find_largest_scans(dag)` | `dag` where `node_type == "Scan"` | list of `{path, bytes, records, query_id, node_id}` sorted by bytes desc |
| `find_repeated_scans(dag)` | derived from above | list of `{path, scan_count, query_ids}` where count > 1 |
| `find_long_running_nodes(dag, threshold_min)` | `dag` | list of `{node_name, node_type, duration_min, query_id}` |
| `find_cartesian_joins(dag)` | `dag` where `node_type == "BroadcastNestedLoopJoin"` | list of `{node_id, query_id, join_type, join_condition}` |
| `find_spill(combined, threshold_bytes)` | `combined` grouped by stage_id | list of `{stage_id, memory_spill, disk_spill}` |
| `find_shuffle_heavy_stages(combined, threshold_bytes)` | `combined` grouped by stage_id | list of `{stage_id, shuffle_read, shuffle_write}` |

Key reuse from existing code:
- `sparkparse.models.NodeType` — all node type filtering
- `sparkparse.models.NODE_TYPE_DETAIL_MAP` — dispatch to correct detail model
- `sparkparse.parse.deserialize_scan_detail()` — parse the `details` JSON string for Scan nodes

**`tests/test_analyze.py`** (new)

Use existing `tests/data/full_logs/` fixtures:
- `to_plan_summary()` returns a dict with `log`, `queries`, `stages` keys
- `queries[n].nodes` contains the right node types for the fixture
- `find_cartesian_joins()` on the `nested_loop_join` fixture returns at least one result
- Plan summary is valid JSON (no unserializable types)

### Verification

```bash
uv run pytest tests/test_analyze.py -v
# Manual check:
sparkparse analyze --log-dir tests/data/full_logs/ | python -m json.tool
```

---

## PR 4: CLI Improvements and `analyze` Command

**Branch:** `feat/cli-improvements`

### Changes

**`sparkparse/app.py`**
- Add `analyze` command that calls `get_parsed_metrics()` then `analyze()` and prints JSON or
  formatted text to stdout (or writes to a file with `--out-file`)
- Add `help=` strings to all `typer.Option()` / `typer.Argument()` calls in `get` and `viz`
- Replace `welcome` command with a `--version` option using an eager callback

Usage after this PR:

```bash
# Pipe to LLM
sparkparse analyze --log-dir ./logs | pbcopy

# Write to file
sparkparse analyze --log-dir ./logs --out-file analysis.json

# Human-readable
sparkparse analyze --log-dir ./logs --format text
```

**`sparkparse/capture.py`**
- Replace `print()` statements (lines 86-87) with `logging.getLogger(__name__).info()`
- Change `action` parameter type to `Literal["viz", "get", "analyze"]`
- Add `"analyze"` as a valid action in `__exit__`: calls `get()` then `analyze()` and stores result
  in `self._analysis`
- Rename the SparkSession created by `capture_context(spark=None)` from `"temp"` to
  `"sparkparse_capture"` for clarity in the Spark UI

### Verification

```bash
sparkparse analyze --help
sparkparse analyze --log-dir tests/data/full_logs/
sparkparse analyze --log-dir tests/data/full_logs/ --format text
```

---

## PR 5: Cloud Storage Support

**Branch:** `feat/cloud-storage`

### Problem

On ephemeral clusters (Databricks, EMR, Dataproc), local disk is gone when the cluster
terminates. Currently:
- `capture.py` writes to `tempfile.mkdtemp()` — local only
- `parse_log()` reads via `open()` — local paths only
- `write_dataframe()` writes CSV/JSON/Parquet to local paths
- `shutil.rmtree` and `shutil.copy2` in `capture.py` break on cloud URIs

Spark can already write event logs directly to cloud storage
(`spark.eventLog.dir = s3://my-bucket/logs/` works via Hadoop connectors), so the gap is
sparkparse's own I/O not following suit.

### Approach: `fsspec` as the filesystem abstraction

`fsspec` is already a transitive dependency of Polars and Pandas. It provides a uniform
`open()` / `ls()` / `rm()` interface across `s3://`, `abfss://` (Azure), `gs://`, `dbfs:/`,
and local paths — no cloud-specific SDK needs to be added to the package itself. Users
install the relevant fsspec backend for their cloud (`s3fs`, `adlfs`, `gcsfs`) alongside
their cluster's existing credentials.

Databricks note: DBFS paths (`/dbfs/...` or `dbfs:/...`) mount as local filesystem paths
on the driver, so they work without fsspec. Unity Catalog volumes (`/Volumes/...`) are
also local-mountable. Direct cloud paths (`s3://`, `abfss://`) require Hadoop credentials
to be configured on the cluster, which Databricks handles through cluster policies or
environment variables.

### Changes

**`pyproject.toml`**
- Add `fsspec>=2024.1.0` to `[project.dependencies]` (lightweight, already transitive)
- Add optional cloud extras so users can pull in the right backend:
  ```toml
  [project.optional-dependencies]
  s3 = ["s3fs>=2024.1.0"]
  azure = ["adlfs>=2024.1.0"]
  gcs = ["gcsfs>=2024.1.0"]
  cloud = ["s3fs>=2024.1.0", "adlfs>=2024.1.0", "gcsfs>=2024.1.0"]
  ```

**`sparkparse/storage.py`** (new)

Path-agnostic I/O utilities. All other modules go through this instead of `open()`,
`os.makedirs()`, `shutil`, or `pathlib.Path` directly.

```python
def is_cloud_path(path: str) -> bool:
    return path.startswith(("s3://", "abfss://", "gs://", "dbfs:/"))

def open_file(path: str, mode: str = "r") -> IO:
    """Open a local or cloud file. Cloud paths require the relevant fsspec backend."""
    if is_cloud_path(path):
        import fsspec
        return fsspec.open(path, mode).open()
    return open(path, mode)

def write_text(path: str, content: str) -> None:
    with open_file(path, "w") as f:
        f.write(content)

def list_files(path: str, pattern: str = "*") -> list[str]:
    if is_cloud_path(path):
        import fsspec
        fs, _ = fsspec.core.url_to_fs(path)
        return [fs.unstrip_protocol(p) for p in fs.glob(f"{path.rstrip('/')}/{pattern}")]
    return [str(p) for p in Path(path).glob(pattern)]

def copy_file(src: str, dst: str) -> None:
    """Copy between any combination of local/cloud paths."""
    if is_cloud_path(src) or is_cloud_path(dst):
        import fsspec
        fsspec.copy(src, dst)
    else:
        shutil.copy2(src, dst)

def remove_dir(path: str) -> None:
    if is_cloud_path(path):
        import fsspec
        fs, _ = fsspec.core.url_to_fs(path)
        fs.rm(path, recursive=True)
    else:
        shutil.rmtree(path)
```

**`sparkparse/parse.py`**
- Replace `open(log_path)` in `parse_log()` with `storage.open_file(log_path)`
- Replace `Path(log_dir).glob("*")` in `get_parsed_metrics()` with `storage.list_files(log_dir)`

**`sparkparse/common.py`**
- Replace `os.makedirs()` in `resolve_dir()` with a cloud-aware mkdir (no-op for cloud paths,
  since cloud storage has no real directories)
- Update `write_dataframe()` for CSV and JSON: Delta and Parquet already handle cloud paths
  natively through PySpark; CSV and JSON use Python's `open()` and need to go through
  `storage.write_text()` instead

**`sparkparse/capture.py`**
- Accept cloud URIs as `temp_dir` (e.g., `s3://my-bucket/sparkparse-logs/run-123/`)
- Replace `os.makedirs(self.temp_dir)` with `storage.ensure_dir(path)` (no-op for cloud)
- Replace `Path(self._log_dir).glob("*")` with `storage.list_files(self._log_dir)`
- Replace `shutil.copy2` with `storage.copy_file`
- Replace `shutil.rmtree` with `storage.remove_dir`
- When `temp_dir` is a cloud URI, `spark.eventLog.dir` is set to that URI directly — Spark
  writes the log there via Hadoop connectors, no local staging needed

**`sparkparse/analyze.py`** (PR 3, but extend here)
- `to_plan_summary()` gains an optional `out_path: str | None = None` parameter
- When provided, writes the JSON summary via `storage.write_text(out_path, json_str)`

### Databricks usage pattern after this PR

```python
import sparkparse

# Logs and summary written to S3; cluster can terminate safely after
with sparkparse.capture_context(
    spark=spark,
    action="analyze",
    temp_dir="s3://my-bucket/sparkparse/job-2024-01-15/",
) as cap:
    df.groupBy("customer_id").agg(F.sum("amount")).write.parquet("s3://...")

summary = cap._analysis  # dict; already written to S3 by capture.__exit__
```

### Verification

```bash
uv run pytest tests/test_storage.py -v
# Cloud path parsing is pure-Python and testable without credentials:
# - is_cloud_path("s3://bucket/key") → True
# - is_cloud_path("/local/path") → False
# - list_files behavior with a local temp dir
```

Manual integration test (requires cloud credentials):
```bash
sparkparse get --log-dir s3://my-bucket/spark-logs/ --out-dir s3://my-bucket/parsed/
sparkparse analyze --log-dir s3://my-bucket/spark-logs/ --out-file s3://my-bucket/summaries/latest.json
```

---

## PR 6: Performance History and Alerts

**Branch:** `feat/history-and-alerts`

### Design

Two concerns kept separate:

1. **`sparkparse/history.py`** — append-only run history. Writes a small fixed-schema record
   per job run to a persistent store. Reads history back for trend queries.

2. **`sparkparse/alerts.py`** — regression detection. Compares the current run record against
   historical records for the same `log_name` and fires configured alerts on regressions.

The history store is not the place for full plan summaries — those are large and
query-specific. The history record is a compact numeric snapshot: the metrics you'd want
to plot as a time series across hundreds of runs.

### History record schema

One row per run, fixed schema, stored in `sparkparse/models.py` as a Pydantic model:

```python
class RunRecord(BaseModel):
    run_id: str             # uuid4
    run_at: datetime        # UTC, when sparkparse processed the log
    log_name: str           # stable job identifier, set by the caller
    duration_s: float       # total wall time across all queries
    bytes_read: int
    bytes_written: int
    shuffle_bytes: int
    spill_bytes: int
    n_queries: int
    n_stages: int
    n_tasks: int
    n_cartesian_joins: int  # count of BroadcastNestedLoopJoin nodes
    max_node_duration_min: float
    max_scan_bytes: int
```

`log_name` is the key for grouping history — it should be a stable identifier for the
job (e.g. `"nightly_customer_agg"`), not the event log filename which includes a
timestamp. The caller sets this explicitly.

`RunRecord` is derived from `ParsedLogDataFrames` by `history.record_from_dfs()`, which
reuses the same accumulator cross-referencing already done in `analyze.py`.

### Storage format

**Primary: Delta Lake** — natively append-only, ACID, supports concurrent writers from
multiple cluster nodes, and is already a supported output format in sparkparse. Works
locally and on cloud storage via PR 5's `storage.py`. Schema evolution is handled
automatically by Delta.

**Fallback: JSONL** — one JSON object per line, used when Delta is unavailable (no
PySpark, or `delta-spark` not installed). Append is a simple file write; reads scan
the whole file.

Format is selected automatically based on whether `delta-spark` is importable, or can
be forced via a `format` parameter.

### `sparkparse/history.py`

```python
def append(record: RunRecord, history_path: str, format: str = "auto") -> None:
    """Append a RunRecord to the history store at history_path."""

def read(history_path: str, log_name: str | None = None, last_n: int | None = None) -> pl.DataFrame:
    """Read history records, optionally filtered by log_name and/or limited to last N rows."""

def record_from_dfs(dfs: ParsedLogDataFrames, log_name: str) -> RunRecord:
    """Derive a RunRecord from parsed DataFrames. Reuses analyze.py helpers."""
```

`history_path` accepts local paths and cloud URIs (via PR 5's `storage.py`). For Delta,
it's a directory; for JSONL, it's a file path.

### Alert configuration

Defined as a list of rules, loaded from a TOML file or passed as a Python dict.
Each rule targets a single metric on a single `log_name`:

```toml
# sparkparse-alerts.toml

[[alerts]]
name = "duration_regression"
log_name = "nightly_customer_agg"   # matches RunRecord.log_name
metric = "duration_s"
condition = "pct_increase"          # pct_increase | absolute_increase | threshold
threshold = 0.20                    # 20% slower than baseline
window = 5                          # baseline = mean of last 5 runs
severity = "warning"                # warning | critical
on_trigger = "log"                  # log | raise | file

[[alerts]]
name = "spill_alert"
log_name = "nightly_customer_agg"
metric = "spill_bytes"
condition = "threshold"             # fire if current value exceeds threshold
threshold = 1073741824              # 1 GiB
severity = "critical"
on_trigger = "raise"

[[alerts]]
name = "cartesian_join_check"
log_name = "nightly_customer_agg"
metric = "n_cartesian_joins"
condition = "threshold"
threshold = 0                       # fire if any cartesian joins appear
severity = "critical"
on_trigger = "raise"
```

`condition` types:
- `pct_increase` — `(current - baseline) / baseline > threshold`
- `absolute_increase` — `current - baseline > threshold`
- `threshold` — `current > threshold` (no historical comparison needed)

`baseline` for windowed conditions is the mean of the last `window` runs for the same
`log_name`, excluding the current run. If fewer than `window` runs exist, the available
runs are used (no alert suppression during warmup).

`on_trigger` dispatch:
- `"log"` — `logging.warning()` or `logging.error()` depending on severity
- `"raise"` — raises `SparkparseAlertError(alert_name, metric, current, baseline)`
- `"file"` — writes a JSON alert record to `alert_output_path` (set globally in config);
  supports cloud paths via `storage.write_text()`

### `sparkparse/alerts.py`

```python
class AlertConfig(BaseModel):
    name: str
    log_name: str
    metric: str
    condition: Literal["pct_increase", "absolute_increase", "threshold"]
    threshold: float
    window: int = 10
    severity: Literal["warning", "critical"] = "warning"
    on_trigger: Literal["log", "raise", "file"] = "log"

def load_alert_config(path: str) -> list[AlertConfig]:
    """Load alert rules from a TOML file."""

def check_alerts(
    record: RunRecord,
    history: pl.DataFrame,
    alerts: list[AlertConfig],
    alert_output_path: str | None = None,
) -> list[dict]:
    """
    Evaluate all alert rules against the current record and history.
    Fires on_trigger actions for any triggered alerts.
    Returns list of triggered alert dicts (empty if none triggered).
    """
```

### Integration with `capture.py`

`SparkparseCapture` gains two optional parameters:

```python
SparkparseCapture(
    action="get",
    history_path="s3://my-bucket/sparkparse-history/",  # where to append
    log_name="nightly_customer_agg",                     # stable job identifier
    alert_config="s3://my-bucket/sparkparse-alerts.toml",  # optional
)
```

In `__exit__`, after parsing:
1. Call `history.record_from_dfs(dfs, log_name)` → `RunRecord`
2. Call `history.append(record, history_path)` → writes to history store
3. If `alert_config` is set: load config, `history.read(history_path, log_name)`,
   `alerts.check_alerts(record, history_df, alert_rules)` → fires triggers

### New CLI commands

**`sparkparse history`** — query the history store:

```bash
# Show last 20 runs for a job
sparkparse history --history-path s3://bucket/history/ --log-name nightly_customer_agg --last 20

# Show all runs (tabular output)
sparkparse history --history-path ./history/
```

**`sparkparse check-alerts`** — run alert checks against the latest run:

```bash
sparkparse check-alerts \
  --history-path s3://bucket/history/ \
  --log-name nightly_customer_agg \
  --alert-config sparkparse-alerts.toml
```

### New files

| File | Purpose |
|---|---|
| `sparkparse/history.py` | `append`, `read`, `record_from_dfs` |
| `sparkparse/alerts.py` | `AlertConfig`, `load_alert_config`, `check_alerts`, `SparkparseAlertError` |

### Changes to existing files

| File | Change |
|---|---|
| `sparkparse/models.py` | Add `RunRecord` Pydantic model |
| `sparkparse/capture.py` | Add `history_path`, `log_name`, `alert_config` params; call history/alerts in `__exit__` |
| `sparkparse/app.py` | Add `history` and `check-alerts` commands |

### Verification

```bash
uv run pytest tests/test_history.py tests/test_alerts.py -v
```

Key test cases:
- `record_from_dfs()` produces a valid `RunRecord` from each of the three fixture logs
- `append()` + `read()` round-trips correctly for both Delta and JSONL formats
- `check_alerts()` with a `threshold` rule fires when the metric exceeds the threshold
- `check_alerts()` with a `pct_increase` rule fires when current > baseline × (1 + threshold)
- `on_trigger = "raise"` raises `SparkparseAlertError`
- `on_trigger = "log"` does not raise, emits a log record
- Alert rules with `window` larger than available history use available runs without error
