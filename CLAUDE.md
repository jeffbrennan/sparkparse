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
  sparkparse/parse.py          – parse raw log into ParsedLog (Pydantic model)
        │
        ▼
  sparkparse/clean.py          – transform ParsedLog into two Polars DataFrames:
        │                         .dag      – one row per physical plan node per query
        │                         .combined – one row per task with full job/stage/query context
        ▼
  sparkparse/models.py         – all Pydantic models and the NodeType / NodeTypeDetailMap
        │
        ├──▶ sparkparse/app.py       – Typer CLI (get, viz commands)
        ├──▶ sparkparse/dashboard.py – Dash app (multi-page: home, summary, dag)
        └──▶ sparkparse/capture.py   – context manager / decorator for live Spark sessions
```

## Key files

| File | Purpose |
|---|---|
| `sparkparse/models.py` | All Pydantic models. `NodeType` enum (30+ values), `ParsedLog`, `ParsedLogDataFrames`, `NODE_TYPE_DETAIL_MAP` (node type → detail model class) |
| `sparkparse/parse.py` | `get_parsed_metrics()` is the main entry point. `parse_spark_ui_tree()` converts indented ASCII plans to node graphs. `parse_log()` orchestrates everything. |
| `sparkparse/clean.py` | `log_to_dag_df()` and `log_to_combined_df()` produce the two output DataFrames. `get_readable_size()` and `get_readable_timing()` are Polars expression helpers. |
| `sparkparse/app.py` | Typer CLI. `get` → parses and writes output files. `viz` → launches dashboard. |
| `sparkparse/capture.py` | `SparkparseCapture` context manager/decorator. Stops the active session, restarts it with event logging enabled, then processes logs on `__exit__`. |

## Data model

### `dag` DataFrame columns (per physical plan node per query)
- `query_id`, `query_function`, `query_header`, `query_start/end_timestamp`, `query_duration_seconds`
- `node_id`, `node_type`, `node_name`, `child_nodes` (comma-separated string), `whole_stage_codegen_id`
- `details` — JSON string; deserialize with the appropriate model from `NODE_TYPE_DETAIL_MAP`
- `accumulator_totals` — list of structs: `{metric_name, metric_type, value, readable_str, unit}`
- `node_duration_minutes`

### `combined` DataFrame columns (per task)
- `log_name`, `parsed_log_name`, `query_id`, `query_function`
- `query/job/stage/task` start/end timestamps and duration_seconds
- `task_id`, `executor_id`, `nodes` (list of physical plan nodes for this task)
- Executor metrics: `executor_run_time_seconds`, `executor_cpu_time_seconds`, `jvm_gc_time_seconds`, `peak_execution_memory_bytes`
- Input/output: `bytes_read`, `records_read`, `bytes_written`, `records_written`
- Shuffle: `shuffle_remote_bytes_read`, `shuffle_local_bytes_read`, `shuffle_bytes_written`
- Spill: `memory_bytes_spilled`, `disk_bytes_spilled`

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
uv run pytest tests/ --ignore=tests/test.py --ignore=tests/test_capture.py -v

# full integration tests (requires a local JVM + PySpark)
uv run pytest tests/test.py tests/test_capture.py -v
```

Test fixtures live in `tests/data/`:
- `tests/data/full_logs/` — three real Spark event logs for end-to-end testing
- `tests/data/test_*_parsing/` — extracted JSON/TXT fragments for unit testing specific parsing steps

## Development commands

```bash
uv sync --dev                          # install including dev deps
uv run ruff check sparkparse/ tests/   # lint
uv run ruff format sparkparse/ tests/  # format
uv run pyrefly check sparkparse/       # type check
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
    severity or pre-classified findings; the LLM draws its own conclusions
  - `find_*()` helpers — programmatic analysis functions for interactive/notebook use
    (`find_cartesian_joins`, `find_largest_scans`, `find_repeated_scans`, `find_spill`, etc.)
- Add `tests/test_analyze.py`

### PR 4 — CLI improvements
- Add `sparkparse analyze` command (prints JSON findings, pipeable to LLM)
- Improve help strings on existing commands
- Add `"analyze"` action to `capture.py`
- Replace `print()` with `logging` in `capture.py`

## Known quirks

- `capture.py` stops and restarts the SparkSession to enable event logging — this resets
  all in-memory cached DataFrames. Warn users accordingly.
- `test.py` and `test_capture.py` in `tests/` are integration tests that spin up a local
  SparkSession. They are slow and cannot run in CI without a JVM available.
- `falsa` is listed in `[project.dependencies]` but its usage in the codebase is unclear —
  verify before removing.
- The `details` column stores Pydantic model data as a JSON string (not a Polars struct)
  to keep the schema flexible across 25+ node types.
