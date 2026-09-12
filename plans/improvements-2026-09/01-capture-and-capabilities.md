# 01 — Safe capture and an explicit capability contract

Status: proposed. Priority: P0. Primary files: `capture.py`, `models.py`,
`connect.py`, `analyze.py`, `history.py`, and public exports.

## Evidence and problem

`SparkparseCapture.__enter__` stops the supplied classic session and recreates it.
Exit stops that session and assigns the already-stopped original object back.
Existing DataFrames, caches, and temporary views cannot be assumed to survive.
Any exception accessing `sparkContext` is treated as serverless detection, including
unrelated failures. Connect capture returns early in `__exit__`, bypassing analysis,
history, alerts, and visualization. `_record_history_and_alerts` also requires a log
directory, which a Connect capture need not have.

`ParsedLogDataFrames` does not convey which metrics are available. Empty task frames
are consequently indistinguishable from genuinely zero task activity. The classic
and Connect frame schemas are not identical (for example, classic DAG accumulators
are absent in Connect). A broad claim of identical schemas should be replaced by a
defined common contract plus optional tables/fields.

## Design

Introduce a versioned capture result containing `dag`, `combined`, metadata,
capabilities, and diagnostics. Retain `ParsedLogDataFrames` compatibility through a
documented adapter and retain `_parsed_logs` temporarily as an alias. Expose public
`cap.dfs`, `cap.analysis`, `cap.result`, `cap.last_record`, and `cap.triggered_alerts`.
Use one source-neutral finalizer for get/analyze/report, history, and alerts.

Metadata should contain capture/run ID, source application/session ID if observed,
backend, observed client/runtime versions, capture start/end, status
(`complete`, `partial`, `failed`), schema version, and workload label. Keep transport,
compute type, and access mode separate; unknown values remain unknown.

Capabilities need per-query coverage, not just one backend boolean: plan structure,
operator metrics, query elapsed time, task metrics, stage timing, scan details,
and join details. Track available/partial/unavailable plus reason and source.
For numeric results preserve measured zero, unavailable (`null`), and not applicable.
An empty finding list alone must not imply a successful assessment.

Use `(capture_id, query_id, plan_version, node_id)` for plan identity and retain
source execution IDs. Introduce stage/task attempt identity for classic data in 04.
Do not use display names as primary keys. Centralize typed empty-frame constructors.

Backends should expose detect/start/finish operations behind an internal interface:

- Existing event logs: parse a supplied source without owning a session.
- Borrowed classic session: attach to already-configured logging, track the capture
  scope, and handle incomplete/unflushed logs explicitly. If unavailable, explain
  startup configuration and allow post-run ingestion; never silently restart.
- Owned local session: an explicit opt-in helper may create and stop its own session.
- Connect: capability detection and guarded adapter from 02.

Allow `backend="auto"` with an explicit override. Prefer supported detection APIs
where the installed version offers them; distinguish unsupported APIs from broken
connections. Report unsupported capture setup before the user's workload executes.
Classic scope attribution needs execution/job identifiers where obtainable; a wall
clock interval alone cannot reliably separate concurrent workloads. Report ambiguity.

Restore hooks and release owned resources in `finally`. Preserve the user's original
exception if finalization also fails; retain partial metrics with diagnostics.
Separate parser strictness from a proposed capture-error policy. Reject or define
nested capture deliberately, and create fresh state for each decorator invocation.

## Increments and acceptance

1. Add metadata/capability models and adapters. Existing constructors still work;
   legacy data has unknown coverage rather than assumed completeness. Contract tests
   cover empty, classic, and Connect-shaped results and serialization round trips.
2. Refactor the common finalizer. Mock both backends and verify get/analyze/history/
   alerts run equivalently without requiring a log directory. Invalid actions fail
   at construction. Missing coverage produces diagnostics, never invented totals.
3. Replace destructive default session handling. A supplied session's `stop` is
   never called; pre-existing DataFrames/views remain usable. Test exception paths,
   reused decorators, empty capture, nested capture policy, and cleanup failures.

Migration: document the classic startup logging requirement and explicit owned-session
option as a behavior change. Do not promise live classic log flushing until tested.
Serverless acceptance uses 05; classic managed access-mode validation remains separate.
