# sparkparse

identify spark bottlenecks without breaking your neck

![example](docs/sparkparse.png)

## what it does

sparkparse parses Apache Spark event logs and provides:

- an interactive Dash dashboard for exploring query plans, stage timelines, and task metrics
- a CLI for parsing logs into structured Polars DataFrames (CSV, Parquet, Delta, JSON output)
- a context manager / decorator for capturing logs from an active SparkSession
- (planned) an `analyze` command that produces structured JSON findings for LLM-assisted analysis

## install

```bash
pip install sparkparse
```

## usage

### CLI

```bash
# parse logs and write output files
sparkparse get --log-dir ./logs --out-format parquet

# launch the dashboard
sparkparse viz --log-dir ./logs

# (planned) produce LLM-friendly analysis JSON
sparkparse analyze --log-dir ./logs
```

### context manager

```python
import sparkparse

with sparkparse.capture_context(spark=spark, action="get") as cap:
    df.groupBy("id").count().show()

parsed = cap.dfs  # ParsedLogDataFrames with .dag and .combined
result = cap.result  # dataframes, metadata, capabilities, and diagnostics
```

Capture borrows a supplied Spark session and never stops or recreates it. Borrowed
classic sessions must have event logging enabled before the workload starts. When
sparkparse should own a local session, opt in explicitly with
`capture_context(own_session=True)`. Connect captures expose the same result contract,
with unavailable task/stage telemetry called out in `result.capabilities` rather than
represented as zeroes.

Use `backend="classic"` or `backend="connect"` to override detection. For post-run
ingestion without a Spark session, use `backend="event_log", log_file="/path/to/log"`.
Borrowed captures select the current application's log; use an explicit `log_file`
when its filename cannot be identified. They report ambiguous capture scope and
may observe an incomplete log. Owned captures stop their session before parsing.

Capture failures raise by default; `capture_errors="record"` retains diagnostics
and a partial result instead. This is independent of parser `strict=True`. Failed
captures retain their temporary logs. User workload exceptions always take precedence.
`action="viz"` produces an in-memory HTML report in `cap.report`, including coverage,
on either backend; it does not launch a dashboard server. Capture-result JSON uses
Arrow IPC tables to preserve schemas, including empty tables and nested metrics.

### decorator

```python
import sparkparse

@sparkparse.capture(spark=spark, action="get")
def run_job(spark):
    spark.read.parquet("s3://bucket/data/").groupBy("id").count().show()

result, cap = run_job()
```

## design goals

- simplified UI that highlights bottlenecks and their causes
- node drill-down for detailed information and metric distribution
- generation of base models and DataFrames for extensible analysis
- LLM-friendly analysis output for automated bottleneck detection

## development

See the [September 2026 review and implementation queue](plans/improvements-2026-09/README.md)
for prioritized standard/serverless parity and developer experience improvements.
[IMPLEMENTATION.md](IMPLEMENTATION.md) contains the earlier roadmap.

```bash
# install with dev dependencies
uv sync --dev

# lint + unit tests (excludes Spark-dependent integration tests)
just ci

# full tests including Spark integration tests
just ci-full
```

## TODOs

- [x] structured node details like project columns and scan sources
- [x] task box plots on hover
- [x] metric capture via context manager / decorator
- [ ] hotspot highlighting by metrics other than duration (spill, records, etc.)
- [ ] `analyze` command with LLM-friendly JSON output
- [ ] reading from cloud storage
- [ ] ruff + pyrefly CI
