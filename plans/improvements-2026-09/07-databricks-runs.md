# 07 — Databricks run reports and commit performance experiments

Status: ready for implementation. Priority: P1.
Dependencies: the capture, analysis, artifact and history foundations in briefs
03, 05 and 06. Preserve the existing parsing, capture and history APIs.

## Objective and scope

Implement **collect existing runs → compare variants → plot locally**. Accept a
Databricks job or run ID, collect execution metadata and available query metrics,
and produce a report that an agent can use to identify bottlenecks and evaluate
performance changes across commits and configurations.

Support workflow timing, I/O and disk-spill analysis through Jobs and serverless
Query History APIs without adding capture code to the workload. Record runs launched
externally, including through the Databricks CLI. Use one CLI transport, a configurable
local file store, shared comparison functions and three dashboard views.

## Repository integration

Add Databricks collection and experiment commands alongside the existing event-log
commands in `sparkparse/app.py`. `databricks.yml` contains demo and validation job
definitions; keep job deployment separate from report collection. Reuse these modules:

- `sparkparse/analyze.py`: metric conventions, evidence-based findings and coverage.
- `sparkparse/artifact.py`: portable capture loading for integrations outside this release.
- `sparkparse/history.py` / `alerts.py`: nullable-record and assessment conventions.
- `sparkparse/models.py`: provenance and metric models. Keep its query-span
  `duration_s` separate from the new workflow elapsed measure.
- `sparkparse/dashboard.py` and `pages/`: Dash/Plotly components and local visualization.

Implement explicit trial comparisons separately from the existing latest-capture
history command. Apply the eligibility and comparability rules in section C;
physical-plan matching must not exclude an optimization that changes the plan.
Keep workflow tasks, Spark task attempts and query aggregates as distinct entities.

## First-release interface

Implement the following command interface. An experiment is identified by its local
directory; its name is metadata in that directory's manifest.

```bash
# Collect a single run, save a portable snapshot, print agent-friendly JSON.
sparkparse databricks analyze --run-id 123 --profile free --out runs/123

# Resolve the latest terminal run, including failures; report the chosen ID.
sparkparse databricks analyze --job-id 456 --profile free --out runs/latest

# Record existing runs in a named experiment; executed Git revision is automatic
# when the run reports it. --revision supplies an explicitly asserted fallback.
sparkparse databricks analyze --run-id 123 --profile free \
  --experiment ./experiments/join-tuning --variant baseline
sparkparse databricks analyze --run-id 124 --profile free \
  --experiment ./experiments/join-tuning --variant broadcast --revision abc123

# Compare explicit trials and view all recorded trials, entirely offline.
sparkparse experiments compare ./experiments/join-tuning --baseline 123 --candidate 124
sparkparse experiments viz ./experiments/join-tuning
```

Require exactly one of job ID/run ID and at most one of `--out`/`--experiment`.
Without either destination, analysis only emits the report. JSON is the default;
`--format text` renders the same facts and comparisons. Progress goes to stderr.
A valid partial report exits 0 and exposes coverage; collection failure exits 1;
invalid arguments exit 2. A failed workload can still produce a valid report.

Accept parent job-run IDs. For a task-run ID, return a message identifying the parent
run where available and explaining the required input. Analyze
never launches, waits for, repairs or cancels a workload. Active runs yield a labeled
partial snapshot; rerunning the same command refreshes it. No polling mode in v1.

## A — Verify the collection contract, then implement run reports

Verify the collection contract before implementing the dashboard. Exercise the CLI
against one retained serverless multi-task run: Jobs pagination, Query History discovery and
metrics, task-run correlation, Git snapshot, and compute/environment fields. Save
sanitized fixtures. Use an existing run if available; otherwise validate with a
separately bounded smoke job. Document the minimum tested CLI version, observed
response shapes, access requirements and fields unavailable in the test environment.

Use the installed Databricks CLI and its existing profile authentication for all
remote calls, including REST calls through `databricks api` if a generated command
lacks the required filters. Use argument arrays and structured JSON, never shell
interpolation or CLI-table parsing. One small client boundary is enough for test
fakes. Keep normalization pure and independent of the CLI process.
Set a collection deadline, bounded page/request counts and bounded rate-limit
retries; persist the limits and any incomplete collection in the report.

Collect job-run metadata, task identities/status/timings, repair/attempt identity,
run-time Git evidence and per-task compute references. Follow Jobs page tokens and
merge paged arrays. Prefer compute snapshots from the run; label subsequent cluster
lookups as current observations. Cache distinct cluster lookups. Missing clusters
or hidden serverless hardware do not prevent a useful report. Preserve runtime,
engine, environment and effective performance mode where exposed. Configuration
values are not measurements of utilization or autoscaled worker counts over time.
Keep workflow elapsed time separate from summed task durations: tasks may overlap.
Report observed slow tasks without claiming an exact critical path or deriving
unreported setup/queue timing from unexplained timestamp gaps.

Task output is enrichment, not the timing source. Default to bounded output retrieval
for failed tasks; `--outputs all` also retrieves successful outputs when desired.
Preserve type, errors, unsupported/missing/truncated states and task-run identity.
Do not infer numeric metrics from arbitrary notebook text. The summary shows a short
failure excerpt and a reference to saved detail rather than every task's output.

Unknown, foreach and nested-job tasks retain their identity and reported metadata.
Do not recursively execute discovery into nested jobs or unroll every iteration in
v1. Mark incomplete child/attempt coverage; exclude such aggregates from claims that
require that detail. Retrying/repairing a run must never silently duplicate a trial
or turn its last successful task attempt into a full-workflow timing measurement.

## B — Add serverless query metrics to the same report

Use Query History REST with metrics included. Discover queries in a bounded run-time
window and paginate; the documented filters do not include job-run ID. Match returned
`query_source.job_info.job_task_run_id` to collected task runs. Parent job-run ID can
be absent. Do not filter by run creator as if it were always the execution identity,
require a warehouse ID for serverless, or attribute solely by time overlap.

Keep one observation per query ID, its status/finality, collection time and exact
association evidence. Deduplicate refreshed pages/snapshots. Do not persist unrelated
workspace queries. Surface denied access, missing attribution and exhausted page
budgets. Completed workflow status does not prove query metrics are fully visible;
recollection can enrich the same trial later without adding a new sample.

| Measure | Definition in the first release |
|---|---|
| Workflow elapsed / task execution | Jobs timing, with queue/setup/cleanup separately where reported |
| Query execution | Query History execution time; not workflow elapsed |
| Read bytes | Query History bytes read; keep cache-read evidence separately |
| Written bytes | Remote persistent bytes written, labeled as such |
| Disk spill | Query History bytes temporarily spilled to disk |
| Aggregate Spark task time | Sum of query task execution time, not elapsed time |

Retain available row counts, cache indicators and shuffle-network bytes as additional
facts. Preserve each source definition. Report memory spill and per-executor
distributions as unavailable from this collection path.

Every aggregate carries its metric/source version, unit, aggregation basis, observed
query count, count with that metric, and completeness status. If two of three known
queries report read bytes, expose an **observed subtotal**, not a complete task total.
Exhausted pagination or uncertain discovery cannot become complete coverage merely
because every returned query has a value. A missing query is not a zero-work query.
Never add Query History metrics to equivalent artifact metrics from the same work.

## Shared snapshot and experiment contracts

Use versioned JSON reports, with a compact summary and bounded detail. The envelope
contains identity/run URL, status, timings, tasks, query facts, compute, revision,
coverage/diagnostics, and measured observations with suggested next checks. Facts
such as a slow task or high disk spill are evidence; a proposed cause remains a
hypothesis. No LLM dependency is needed to generate a useful agent report.

Executed revision comes first from `git_source.git_snapshot.used_commit` at its
reported scope. Retain repository identity and full SHA. Branch names, current
local HEAD and requested revisions are not proof of executed code. Workspace
notebooks/wheels may lack Git evidence: retain asserted `--revision`, optional
artifact digest, and unknown/mixed provenance without blocking collection. Preserve
per-task/repair provenance when available; do not attribute a mixed run to one SHA.
Fetching commit subjects, code diffs and repository contents is not required in v1.

An experiment groups **variants** defined by code revision and execution configuration.
The same commit can have multiple variants. Each trial records a
variant label, revision evidence, configuration fingerprint, and optional input
snapshot, warmup flag, correctness outcome, hypothesis and notes. Do not pool differing
configurations just because their commit matches. Restrict each experiment to one
workspace and job, established by its first collected run. Reject incompatible imports.

Persist a small editable `experiment.json` (identity, objective, pinned baseline and
trial references) plus immutable collection snapshots. Recollecting a run appends a
snapshot and updates its reference, not its trial count. Keep snapshot IDs in saved
comparisons so late enrichment does not rewrite an earlier result. Use atomic local
file replacement for manifest updates; support one writer per experiment, no cloud
append/concurrent-writer guarantees. Build Polars tables on load; no separate database,
Delta integration or persistent index is required. Keep existing RunRecord
storage compatible and separate from these workflow snapshots.

Storage location is explicitly configurable through `--experiment PATH` or
`--out PATH`; accept absolute paths or paths relative to the invoking directory.
Do not write to a hidden global store or choose a working-directory default.
For example:

```text
experiments/join-tuning/
  experiment.json
  runs/
    123/
      <snapshot-id>.json
    124/
      <snapshot-id>.json
```

Snapshot IDs are collision-resistant; each snapshot records workspace/run identity,
schema version and collection time. Any bounded output attachments use paths
relative to this directory, so copying it preserves offline access. Standalone
`--out` directories also retain snapshots instead of silently overwriting prior
collections. Keep report refresh atomic and surface invalid/corrupt manifests as
errors. Reject cloud URIs clearly. Retain all snapshots; automatic pruning is outside
this release's scope.

Allowlist useful configuration metadata and exclude credentials before saving.
Bound text and omit full query text by default. Escape notebook/error text in HTML;
it is data, not executable instructions. Optional sanitized raw fixtures are for
replay/debugging, not a default dump of API responses. IDs/timestamps/byte counts keep
exact types; display-unit conversion must not round the stored source measures.

## C — Comparisons and local dashboard

Comparison consumes saved snapshots and a pinned baseline, never an implicit moving
latest-run baseline. Start with explicit trial pairs; then allow explicit baseline
and candidate variant sample groups. Align workflow tasks by stable key; show added
and removed tasks. Physical-plan fingerprints do not define experiment identity.

Show absolute delta, percentage delta (undefined for a zero baseline), individual
trial values, sample count, median and min/max. One run per variant is still useful:
label it a single observation. Do not implement significance tests or automatically
accept/reject commits. Failed, active, warmup and incomplete-objective trials remain
visible but are excluded from default performance aggregates. Display failed trial
counts alongside results so excluding failures does not hide a reliability regression.

Show comparability separately from performance: metric semantics/coverage, revision
confidence, input snapshot, parameters, compute/runtime, cache and correctness
outcomes. Missing evidence yields explicit caveats, not a refusal to show raw deltas.
Use user-declared input identity as evidence; **observed bytes read are an outcome,
not an input fingerprint**—successful pruning is allowed to reduce I/O. Likewise,
compute/configuration changes can be the intended experiment variable: display them
and keep groups separate, rather than forbidding every such comparison. Correctness
status is externally supplied and may be unknown; job success alone is not proof.

Reuse Dash/Plotly under the existing `[viz]` extra. One experiment page needs:

1. **Trend plot:** select metric and workflow/task; show each trial with a
   variant/short-SHA label, median and spread. Separate axes/panels for bytes and
   seconds. Missing metrics are gaps; partial subtotals are visibly marked and not
   mixed with complete totals. Order by first trial or execution time, not SHA.
2. **Baseline table:** per-task absolute values/deltas, plus added/removed tasks and
   excluded counts. Show timing components as columns before adding more charts.
3. **Run detail:** compute/configuration differences, revision evidence, source
   coverage, top expensive queries, failure excerpts and Databricks run links.

Provide filters for variant/commit, task and trial eligibility. Read saved data only,
bind to loopback, and refresh locally on request. Reuse exactly the same comparison
functions in JSON, text and dashboard.
Offer CSV/JSON export of plotted data.

## Out of scope

- Launching, deploying, polling, repairing or cancelling remote workloads.
- Correlating classic event logs or Connect artifacts with workflow runs; automatic
  remote log discovery, artifact-output envelopes and download adapters.
- Full query-profile imports and system-table collection.
- Recursive nested-job/foreach traversal and cross-job/workspace comparisons.
- SDK transports, remote storage, concurrent writers and automatic retention.
- Standalone HTML exports, task heatmaps, code/plan diffs and best-so-far curves.
- Statistical significance tests and automatic acceptance/rejection of code changes.

## Acceptance and validation

Implement A, B and C as separate reviewable changes in that order. A delivers run
timing reports; B adds query metrics; C adds experiment comparisons and visualization.
The feature is complete when all three increments meet the following criteria.

| Area | Required cases |
|---|---|
| Collection | Single/multi-task, paginated arrays, active/failed/skipped, repair/retry, unknown/nested task metadata, missing compute, bounded/truncated outputs, auth and rate limits |
| Query metrics | Pagination, unrelated queries in window, missing parent ID with valid task ID, absent source identity, late/final metrics, partial metric subtotals, duplicate pages, missing versus zero |
| Provenance | Executed Git snapshot, asserted/unknown/mixed revision, same SHA with different configurations, no local checkout |
| Store/compare | Recollection is one trial, pinned snapshots, out-of-order runs, zero baseline, failed/warmup samples, unequal sample counts, missing inputs, intentionally changed compute, reduced read bytes |
| Dashboard | Fixture-verified aggregates/deltas, filters, gaps, escaped text, drilldown, clean offline startup and tabular export |

Validate B against retained serverless executions and save sanitized fixtures; test
classic API metadata where an environment is available. No requirement to provision
classic compute merely to test the serverless workflow. Test offline normalization
and CLI contracts, then inspect the dashboard in a browser. Report offline and live
coverage separately. Do not rerun costly workloads for cosmetic/report changes.

Run the repository's lint, format, type and offline test checks for implementation
changes (`just ci`). Add focused tests for the contracts above. Record the tested
commit and environment alongside results; distinguish new-feature validation from
existing parser/capture coverage.

## API references

- [Jobs CLI](https://docs.databricks.com/aws/en/dev-tools/cli/reference/jobs-commands):
  paged run retrieval and per-task output.
- [Jobs run API](https://docs.databricks.com/api/jobs/v2/get-run):
  run/task provenance and `git_snapshot.used_commit`.
- [Query History API](https://docs.databricks.com/api/query-history/v1/query-history)
  and [CLI](https://docs.databricks.com/aws/en/dev-tools/cli/reference/query-history-commands):
  serverless metrics, discovery filters, pagination and source attribution.
- [Serverless limitations](https://docs.databricks.com/aws/en/compute/serverless/limitations):
  query profiles replace conventional Spark UI/log access.
