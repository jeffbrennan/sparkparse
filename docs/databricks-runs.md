# Databricks run collection

`sparkparse databricks analyze` collects an existing Databricks job/run into a
portable, versioned report. It never launches, waits for, repairs or cancels a
workload. `sparkparse experiments` compares saved reports offline.

## Tested collection contract

| Item | Value |
|---|---|
| Databricks CLI | v1.15.0 |
| Profile used | `free` (serverless; Databricks Free Edition) |
| Validated | 2026-09-13 against retained serverless runs of the bundled bundle jobs |
| Auth | Existing CLI profile; no SDK transport |

Required access: Jobs API read access for the run, and the `query-history` scope
to read Query History with metrics. Git snapshot fields appear only when the job
was configured with a Git source.

### Jobs

- `databricks api get /api/2.1/jobs/runs/get?run_id=<id>&include_resolved_values=true`
  returns identity, `state`/`status`, `run_duration`/`setup_duration`/
  `execution_duration`/`cleanup_duration`, `tasks[]`, `environments[]`, and
  `git_source` when present. Array properties are paged with `next_page_token`.
- `databricks api get /api/2.1/jobs/runs/list?job_id=<id>&limit=<n>` returns
  `runs[]`, `has_more` and `next_page_token`. The generated `jobs list-runs`
  command exposes `--offset` but not the page token, so the raw API is used.
- Task output: `databricks jobs get-run-output <task-run-id>` returns
  `metadata`, `notebook_output{result,truncated}` and optionally `error`/`logs`.
- The generated `jobs get-run` command omits `environments`; the raw API does not.

### Query History

Query History filters use dot notation, for example:

```
/api/2.0/sql/history/queries?include_metrics=true&max_results=100
  &filter_by.query_start_time_range.start_time_ms=<ms>
  &filter_by.query_start_time_range.end_time_ms=<ms>
```

The generated `query-history list` command in v1.15.0 has no filter flags, so
the raw API path is used. The response is
`{res: [QueryInfo...], has_next_page, next_page_token}`. Queries are matched to
collected task runs through `query_source.job_info.job_task_run_id` (a parent
`job_run_id` may be absent). Attribution is never made from time overlap alone.

Metrics observed per query: `read_bytes`, `read_remote_bytes`,
`read_cache_bytes`, `write_remote_bytes`, `spill_to_disk_bytes`,
`task_total_time_ms`, `execution_time_ms`, `total_time_ms`, `rows_read_count`,
`rows_produced_count`, `network_sent_bytes`, `result_from_cache`,
`read_files_count`, `read_partitions_count`.

### Not available from this collection path

- `git_source.git_snapshot.used_commit` on workloads without a configured Git
  source (the bundled bundle jobs did not report one; fixtures cover it).
- Classic cluster details for serverless runs, and cluster snapshots taken at
  run time when only an `existing_cluster_id` is reported (lookups are labelled
  `current_lookup`).
- Memory spill and per-executor distributions; only disk spill is reported.
- Query profiles and full query text (query text is never persisted).

## Report measure semantics

| Measure | Definition |
|---|---|
| Workflow elapsed | `run_duration`; never the sum of task durations (tasks may overlap) |
| Task execution | Per-task `execution_duration`, plus `setup`/`cleanup` |
| Query execution | Query History `execution_time_ms`; not workflow elapsed |
| Read bytes | Query History `read_bytes`; cache reads kept separately |
| Written bytes | Remote persistent bytes (`write_remote_bytes`), labelled as such |
| Disk spill | Query History `spill_to_disk_bytes` |
| Aggregate task time | Sum of query `task_total_time_ms`, not elapsed |

Every aggregate records its source/version, unit, aggregation basis, observed
query count, count with the metric, and a completeness status. A subtotal over
only the queries that reported a metric is `observed_subtotal`, never a complete
total. Missing metrics are `null`; a measured zero stays `0`.

## Usage

```bash
# report only (JSON to stdout, progress to stderr)
sparkparse databricks analyze --run-id 123 --profile free

# save a portable snapshot
sparkparse databricks analyze --run-id 123 --profile free --out runs/123

# resolve the latest terminal run for a job
sparkparse databricks analyze --job-id 456 --profile free --out runs/latest

# record trials in an explicit experiment
sparkparse databricks analyze --run-id 123 --profile free \
  --experiment ./experiments/join-tuning --variant baseline
sparkparse databricks analyze --run-id 124 --profile free \
  --experiment ./experiments/join-tuning --variant broadcast --revision abc123

# compare and view saved trials entirely offline
sparkparse experiments compare ./experiments/join-tuning --baseline 123 --candidate 124
sparkparse experiments viz ./experiments/join-tuning
```

An experiment directory holds an editable `experiment.json` plus immutable
snapshots:

```text
experiments/join-tuning/
  experiment.json
  runs/
    123/<snapshot-id>.json
    124/<snapshot-id>.json
```

Recollecting a run appends a snapshot and updates its trial reference without
adding a trial. Storage is local only; cloud URIs are rejected. Manifest updates
use atomic local file replacement (one writer per experiment).

Exit codes: `0` for a valid report (including partial/active), `1` for a
collection or experiment failure, `2` for invalid arguments.
