# 04 — Event-log coverage and scalable ingestion

Status: proposed. Priority: P1, with non-AQE support an early correctness fix.
Depends on 01's identity/partial-result contract. Files: `parse.py`, `clean.py`,
`models.py`, `storage.py` and event-log fixtures.

## Evidence

`parse_log` reads all lines into memory, requires ApplicationStart, and only collects
plans from final adaptive-execution updates. SQLExecutionStart is used for timestamps,
not a fallback physical plan; a non-AQE workload can therefore yield “No queries found.”
`get_parsed_metrics` picks the lexicographically last directory entry, not all logical
logs or a reliably newest application; empty local directories index an empty list.
There is no rolled-log/codec assembly here. `parse_task` expects metric sections to
exist. `parse_stage` retains stage ID but not attempt ID; cleaning joins tasks/stages/
jobs on stage ID, risking duplicate accounting for retries or reused stages.

## Design

Define a logical event-log source with ordered segments, application identity,
compression metadata, completeness state, and source URIs. Make file selection
explicit (`--log-file`, documented newest policy, or all applications); ignore marker
files/directories. Preserve output identity across applications whose query IDs repeat.

Parse records incrementally. Support the initial SQLExecutionStart plan and replace
or retain plan versions as AQE updates arrive. Failed and incomplete queries retain
their best available plan and missing end time; absence of AQE is not an error.
Distinguish a truncated trailing record from corruption mid-file. Tolerant mode emits
line/segment diagnostics; strict mode fails with actionable source context.

Add stage attempt, task attempt, partition, status, and failure reason. Separate all
attempt resource usage from successful-output accounting. A shared stage may have
multiple query associations; global totals count physical attempts once. Preserve
association tables rather than multiplying task rows through many-to-many joins.

Implement rolling/codec support based on real Spark 3.5/4.x layout fixtures and
document supported codecs explicitly. Keep fsspec remote reads streaming. Benchmark
before optimizing: incremental parsing alone still retains model/task objects; chunked
task storage or Parquet spill is a later step if memory measurements justify it.

## Increments and acceptance

1. Plan fallback and partial parsing: non-AQE, no queries, failed query, missing end,
   truncated tail, missing task metrics, unknown events, and empty source tests.
2. Identity and attempts: two stage attempts and speculative tasks, reused stages
   across jobs, repeated query IDs across applications. Totals match a hand-computed
   physical-attempt ledger and no duplicate rows arise from associations.
3. Source discovery and streaming: rolled segments, compressed fixture, metadata files,
   explicit file selection, fake cloud filesystem. Compare streamed versus single-file
   output on identical events. Record elapsed time and peak RSS on increasing fixture
   sizes; require no whole-file text copy, document remaining retained-state growth.

Do not claim continuous live ingestion or Structured Streaming parity in this change.
Those require a separate lifecycle and progress-event design. Managed Databricks
classic logging/access permissions must be verified independently of local Spark.
