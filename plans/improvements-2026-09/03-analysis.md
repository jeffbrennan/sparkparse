# 03 — Correct metrics, then deeper analysis

Status: implemented (2026-09-12), offline validation only. Priority: P0 correctness,
P1 depth. Depends on 01–02.

Implementation notes:

- `sparkparse/metrics.py` holds the metric registry; `MetricDefinition`,
  `NormalizedMetric`, `Finding`, `RuleAssessment` and `AnalysisReport` live in
  `models.py`.
- `find_cartesian_joins` no longer counts conditional nested loop joins; those are
  reported by `find_nested_loop_joins` and the `nested_loop_join` rule. Cross-join
  matching is query-scoped.
- Expansion ratios come from the join's immediate inputs, descending only through
  row-preserving operators. Scans are used for lineage only.
- Scan efficiency has no pushed/partition-filter evidence to work from: the parsed
  `ScanDetail` does not retain `PushedFilters`/`PartitionFilters`, so findings are
  worded as "rows read but discarded", not as a pruning verdict.
- Redaction covers node names as well as structured details: Connect embeds the
  server's operator name in the display name for unmapped operators, so the name
  is rebuilt from node id and resolved type with the remainder tokenized.
- Task-backed rules (`spill`, `shuffle_volume`, `task_straggler`, `gc_overhead`)
  verify that the counters they need are actually populated before reporting
  `evaluated`, and count assessed stages rather than stages over threshold.
- Exact integer counts hold end to end: `accumulator_totals` carries an
  `value_exact` `Int64` beside the `Float64` `value` in both the event-log and
  Connect paths, and normalization prefers it. Regression tests cover a patched
  event log and a Connect replay carrying 2**53 + 1.
- AQE initial plans are dropped by `parse.py` (only `isFinalPlan=true` snapshots are
  kept), so `aqe_plan_change` reports optimizer-recorded adjustments on the surviving
  plan and says so in its assessment reason.

Primary files: `analyze.py`, `metrics.py`, `models.py`, `app.py`, `capture.py`, `pages/summary.py`.

## Confirmed gaps

`find_spill` only reads tasks. `find_largest_scans` only attributes task input metrics
to Scan nodes. Neither implements the Connect plan fallback claimed in the old brief.
`to_plan_summary` sums an empty task table to zero and exports node metric values as
display strings, dropping machine-readable units/aggregation. `_get_output_rows`
uses `or`, losing a legitimate zero. Classic cleaning also fills missing durations
with zero.

`find_cartesian_joins` includes every BroadcastNestedLoopJoin, omits explicit
CartesianProduct, and filters discovered Cross joins by node ID without query ID.
Conditional nested-loop joins are not necessarily Cartesian. Row expansion compares
join output with remote source scans, which may precede filters or aggregation;
this is not actual join-input cardinality and does not prove duplicate keys.
Repeated scans are deduplicated by path/query, hiding repeated scans inside one query.

## Metric contract

Introduce a metric registry with canonical name, raw name, source, numeric value,
unit, scope (query/operator/stage/task), aggregation (sum/max/last/unknown), coverage,
and derivation. Use explicit null checks. Keep integer counts exact and timing
precision intact. Preserve unknown metrics for inspection without guessing units.

Add verified Photon and classic aliases for scan/output rows, scan bytes, spill,
shuffle, and timing. A size metric is not automatically scan bytes. `numRowsScanned`
and `numOutputRows` are different concepts. Do not equate memory spill size with disk
spill size or add them as if they represented disjoint physical bytes.

Plan spill can support an operator finding when task data is unavailable, but cannot
invent a stage/task distribution. Avoid summing parent and child cumulative counters
or both sides of a shuffle into a query total without a verified aggregation rule.
When task metrics are attributed to multiple plan nodes, label that attribution as
shared/estimated and avoid counting the same task repeatedly in global totals.

## Findings and deeper analysis

Keep raw plan summary separate from diagnostic findings. Define a result envelope
containing findings AND rule assessment status (`evaluated`, `unsupported`,
`insufficient_data`). A finding has stable rule ID, scoped entity IDs, evidence values,
units, threshold, confidence, caveat, and suggested next investigation. Preserve the
existing DataFrame helpers via wrappers where possible.

First fix CartesianProduct detection, query-scoped identity, and zero preservation.
Separate conditional nested loops from cross joins. Calculate expansion from direct
join inputs when metrics exist; scans identify lineage only. Unknown branch roles or
missing input rows lower coverage. Outer joins and intentional many-to-many joins
need cautious wording: expansion is observed, incorrect keys are a hypothesis.

Then add these rules, each only when its evidence is available:

| Analysis | Evidence | Constraint |
|---|---|---|
| Scan efficiency | scanned/output rows, bytes, pushed/partition filters | No pruning verdict without denominator/filter detail |
| Repeated work | normalized source + filters/projection + plan identity | Cache is a candidate, not an automatic fix |
| Join expansion | immediate input and output rows | Respect join semantics and missing inputs |
| Spill and shuffle | source-specific operator or task counters | Retain scope; avoid duplicate rollup |
| Task stragglers | p50/p95/max duration, partition bytes, attempt state | Label straggler; data skew needs size evidence |
| GC/scheduler overhead | task runtime, GC, elapsed, scheduler timing | Classic-only unless source supplies equivalents |
| AQE changes | initial/final plan and runtime metrics | Do not count every plan snapshot as executed work |

LLM export should include schema version, coverage, numeric metrics with units, stable
IDs, and optional compact/top-N mode with omitted counts. Make path/literal redaction
an export option for sharing real workloads. Recommendations remain distinguishable
from observations. Do not sum operator times to claim a critical path.

## Acceptance / increments

1. Metric normalization and missing-data fixes: equivalent classic/Photon fixtures
   yield equivalent semantic values; 0 stays 0; absent task metrics stay null; plan
   spill is found without creating task rows. Round-trip numeric JSON without loss.
2. Correct existing rules: reused node IDs in two queries, CartesianProduct,
   conditional nested loop, zero-row input, filters before joins, reused exchange,
   repeated scan within one query, and incomplete scan detail all have explicit tests.
3. Add depth and presentation: every new rule has positive, negative, and unsupported
   fixtures. Missing telemetry yields a visible explanation in JSON and UI. All-null
   rankings do not show arbitrary “largest” scans. Existing raw summary remains usable.
