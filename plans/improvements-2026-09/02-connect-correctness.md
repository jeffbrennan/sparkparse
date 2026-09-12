# 02 — Reliable Spark Connect capture

Status: implemented offline; not live-validated. Priority: P0. Depends on 01's
identity and coverage contract.
Primary file: `connect.py`; add `tests/test_connect.py` and sanitized fixtures.

Implementation notes (2026-09-12): the adapter now intercepts action boundaries
(`to_table`, `to_pandas`, `to_table_as_iterator`, `execute_command`,
`execute_command_as_iterator`) plus `_build_metrics` and
`_execute_plan_request_with_metadata`. Hooks are probed for presence, installed
atomically, and the exact originals are restored (no leftover instance attributes) on
setup failure, workload failure, and normal exit. A second capture on one client is
rejected. Metrics are attributed to the execution active on the calling thread, so
plan/metric correlation no longer depends on list position; metric batches are treated
as snapshots (last value per plan ID wins) because pyspark's own
`CollectedMetrics.extract_graph` replaces rather than accumulates repeats. Join keys
are attached only on a logical plan-ID match, a single-join-per-query mapping, or an
operator name that carries them; anything else stays unresolved with a diagnostic, and
joins record `input_roles: unordered`. Unknown operators degrade to `NodeType.Unknown`
with their raw names preserved unless `strict=True`. `query_duration_seconds` is
client-observed elapsed time (capability capped at `partial`, reason states the
transfer inclusion); cumulative operator time stays per node with its source unit.
Connect diagnostics now reach `CaptureResult.diagnostics`, and `source_execution_id`
carries the server operation ID. `probe_connect_support()` reports the client surface
including `DataFrame.executionInfo` availability; the public API was not substituted
for interception because it is scoped to one already-executed DataFrame and cannot
observe SQL commands, writes, or other objects' actions.

Offline validation: `tests/test_connect.py` (39 cases, no Spark/gRPC needed) plus
`tests/data/connect/photon_join_execution.json`, a **synthetic** sanitized fixture in
the `PlanMetrics.to_dict()` shape — it is not a recording of a live Databricks run.
Live validation from 05 (bounded serverless job, recorded real PlanMetrics and version
metadata) is still outstanding.

## Evidence

- Private `_build_metrics` and `to_table` methods are patched with no version guard.
- Each nonempty metrics callback becomes a query. Logical plans are stored separately;
  joins are deduplicated across all queries, then assigned by encounter order with
  the index reset for every query. Different queries can receive another query's keys.
- Child IDs are sorted numerically, which does not establish semantic left/right.
- `_map_node_type` only consults a small map; unknown operators abort the entire result
  even though the classic parser has a tolerant fallback.
- Root `cumulTime` is stored as query elapsed time without establishing that semantic.
- Context reuse retains old query/plan lists. Partial execution data is discarded on
  user exceptions. No dedicated Connect regression tests currently exercise this.

## Design and bounded investigation

First evaluate the installed client's public `DataFrame.executionInfo` surface for
explicit DataFrame capture. Apache Spark documents it as a Connect-only API. This
does not establish Databricks runtime availability or automatic context-wide coverage.
Probe feature presence and behavior; do not replace broad capture with a narrower API
without documenting the action coverage.
[Apache Spark executionInfo](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.executionInfo.html).

Keep private interception behind one tested adapter if needed. Record client version,
check required method signatures, install hooks atomically, and restore exact originals
after setup failure, execution failure, or conversion failure. Reject concurrent/nested
use on one client initially unless ownership and isolation are implemented and tested.

Correlate logical plan, action, and metrics with source operation/execution identifiers
when available. Establish whether response metrics are snapshots or deltas, and merge
accordingly. Retain raw identifiers and provenance. If the source cannot correlate,
mark ambiguous records and leave join details unresolved. Never globally match joins
by list position. AQE and optimizer reordering can defeat logical-to-physical matching
even within one query; only attach keys when a verified mapping supports them.

Keep raw operator names and unknown metrics. Map aliases then try the canonical
`NodeType`; otherwise emit `Unknown` with `RawDetail`, preserving graph edges. Forward
strict mode from the public capture API. Do not label an unknown node as an exchange
merely to make visualization work.

Capture monotonic action elapsed time when an action boundary is intercepted and
UTC timestamps for display. Mark client elapsed time as including transfer/collection;
it is not server execution time. Keep cumulative operator time separately with source
units and aggregation semantics. Graph edges need explicit input roles when observable;
otherwise show unordered children without asserting left/right.

## Increments and acceptance

1. Tolerant mapping, typed raw fixtures, hook lifecycle, and per-entry state reset.
   Test unknown and canonical operators, empty metrics, malformed metrics diagnostics,
   missing methods, setup failure after first hook, repeated contexts, and exceptions.
2. Correlation and topology. Fixture two unrelated joins with different keys, repeated
   actions on the same DataFrame, multiple joins, AQE changes, and interleaved response
   streams. Assert no cross-query key leakage and no duplicate snapshot summation.
3. Timing semantics and public-API capability probe. Test count/collect/show, SQL,
   write and iterator actions as distinct coverage cases. Unsupported actions report
   missing coverage; no additional action is executed just to collect telemetry.

Live validation: one bounded job from 05, save sanitized PlanMetrics and version
metadata for offline replay. Do not require expensive spill/skew workloads. Profile
export enrichment is a later optional adapter: investigate a supported export/API
before promising it, and keep system-table access optional.
