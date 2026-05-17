# sparkparse Implementation Roadmap

This document describes the planned improvements to sparkparse across three high-level goals,
broken into self-contained PRs that can be reviewed and merged independently.

## Goals

1. **Modernize** — UV dev groups, ruff config, pyrefly type checking, more unit tests, CI
2. **Better UX** — improved CLI, cleaner SparkSession wrapper, richer output
3. **LLM-friendly analysis** — structured JSON findings for bottleneck detection (largest tables,
   repeated scans, long-running operations, cartesian joins, high spill, shuffle-heavy stages)

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

### Changes

**`sparkparse/analyze.py`** (new)

Entry point: `analyze(dfs: ParsedLogDataFrames, log_name: str) -> dict`

Returns a structured JSON-serializable dict with two top-level sections:

```json
{
  "metadata": {
    "log_name": "...",
    "analyzed_at": "ISO-8601",
    "n_queries": 5,
    "total_wall_time_seconds": 272.4,
    "total_bytes_read": 3045000000,
    "total_shuffle_bytes": 1200000000
  },
  "findings": [
    {
      "type": "cartesian_joins",
      "severity": "critical",
      "items": [{"node_id": 12, "query_id": 3, "join_type": "Cross", "join_condition": null}]
    },
    {
      "type": "repeated_scans",
      "severity": "warning",
      "items": [{"path": "/data/warehouse/fact_orders", "scan_count": 3, "query_ids": [1, 2, 4]}]
    },
    ...
  ]
}
```

Finding types and their data sources:

| Finding type | Source | Logic |
|---|---|---|
| `largest_scans` | `dag` where `node_type == "Scan"` | deserialize `details` for paths, read `accumulator_totals["size of files read"]` |
| `repeated_scans` | derived from largest_scans | group by path, count > 1 |
| `long_running_nodes` | `dag` | filter `node_duration_minutes > 1.0` (configurable) |
| `cartesian_joins` | `dag` where `node_type == "BroadcastNestedLoopJoin"` | deserialize `details` for join_type and join_condition |
| `spill_operations` | `combined` | group by stage_id, sum `memory_bytes_spilled + disk_bytes_spilled > 10 MiB` |
| `shuffle_heavy_stages` | `combined` | group by stage_id, sum `shuffle_bytes_read + shuffle_bytes_written > 1 GiB` |

Key reuse from existing code:
- `sparkparse.models.NodeType` — all node type filtering
- `sparkparse.models.ScanDetail`, `BroadcastNestedLoopJoinDetail` — detail deserialization
- `sparkparse.models.NODE_TYPE_DETAIL_MAP` — dispatch to correct detail model
- `sparkparse.clean.get_readable_size()`, `get_readable_timing()` — format readable metrics
- `sparkparse.parse.deserialize_scan_detail()` — parse the `details` JSON string for Scan nodes

**`tests/test_analyze.py`** (new)

Use existing `tests/data/full_logs/` fixtures. Parse with `get_parsed_metrics()` then run
`analyze()` and assert:
- Result is a valid dict with `metadata` and `findings` keys
- `nested_loop_join` fixture produces at least one `cartesian_joins` finding
- All findings have `type`, `severity`, `items` keys

### Verification

```bash
uv run pytest tests/test_analyze.py -v
# Manual check:
uv run python -c "
from sparkparse.parse import get_parsed_metrics
from sparkparse.analyze import analyze
import json
dfs = get_parsed_metrics('tests/data/full_logs', log_file='nested_loop_join', out_dir=None, out_format=None)
print(json.dumps(analyze(dfs, 'nested_loop_join'), indent=2))
"
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
