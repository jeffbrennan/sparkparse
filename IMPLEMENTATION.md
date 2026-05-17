# sparkparse Implementation Roadmap

This document describes the planned improvements to sparkparse across three high-level goals,
broken into self-contained PRs that can be reviewed and merged independently.

## Goals

1. **Modernize** — UV dev groups, ruff config, pyrefly type checking, more unit tests, CI
2. **Better UX** — improved CLI, cleaner SparkSession wrapper, richer output
3. **LLM-friendly analysis** — token-efficient structured plan summary for piping to an LLM
   (queries, nodes, durations, bytes, join types, paths — raw facts, no pre-assigned severity),
   plus programmatic `find_*` helpers for interactive analysis in notebooks/scripts

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
