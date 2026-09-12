# 05 — Packaging, CLI, reports, and a practical test matrix

Status: proposed. Priority: P1 (small packaging fixes can precede 01).

## Evidence

Core dependencies declare only Polars/Pydantic, but `__init__.py` imports capture,
which imports PySpark; `common.py` also imports PySpark. `app.py` imports undeclared
Typer and eagerly imports the optional dashboard. The development environment masks
these installation gaps. README CLI examples use `--log-dir`, whereas commands define
`log_dir` as a positional argument. The capture dashboard subprocess also uses that
option. The `timeit` decorator prints to stdout, contaminating piped analysis JSON.

README describes existing analyze/cloud/CI functionality as planned. Old roadmap
guidance is substantially implemented or contradicted. CI runs on PR opened/reopened
but not synchronize, so subsequent pushes do not automatically get fresh checks.
The release workflow rewrites package metadata version while CLI version is hardcoded.
`databricks.yml` hardcodes wheel version/path. There are two current type-check errors
recorded in the review index.

## Implementation increments

1. **Minimal install and CLI.** Declare Typer; lazy-load Spark and visualization at
   feature boundaries, including transitive imports. Audit direct dependencies of
   each extra. Keep base parsing/analyze usable without Spark or Dash. Derive version
   from package metadata. Choose a canonical CLI syntax and optionally support the
   documented `--log-dir` alias. Move timing output to logging/stderr. Validate formats
   and missing paths clearly; analysis text mode must handle unavailable duration.
2. **Repeatable development.** Resolve the two pyrefly errors after checking actual
   pandas shapes. Add PR synchronize checks and concurrency cancellation; keep costly
   remote checks opt-in. Use frozen dependency installation in CI, documented dependency
   update flow, portable JVM setup, and a smoke-tested wheel. Parameterize bundle
   artifact paths from the actual build. Replace stale README claims with tested
   usage and link this queue from the historical roadmap.
3. **Portable capture artifact/report.** Persist manifest + DAG/task tables + diagnostic
   JSON so Connect results can be opened locally without raw event logs. Let the
   dashboard accept this artifact or an in-memory result, not just a raw-log directory.
   Prefer notebook rendering or export on remote compute instead of assuming a local
   browser on port 8050. Show unavailable panels with reasons; keep DAG/details useful
   when task tables are empty. Cache parsed data server-side instead of repeatedly
   parsing and shipping all tasks into browser stores; add bounded drilldown/pagination.

## Acceptance

- Fresh isolated wheel environments for base, `[viz]`, and `[spark]`; base import,
  `--help`, `--version`, parse/analyze work without optional dependencies installed.
- CLI stdout parses with `json.loads` directly; logs appear only on stderr. README
  commands are exercised with tiny fixtures. Version matches wheel metadata.
- Offline suite, lint, format, and type checks pass on the updated PR head.
- Persisted classic and Connect artifacts reopen with the same metrics/coverage;
  empty/partial results and large plans render without task-dependent exceptions.

## Validation matrix and quota budget

| Lane | Environment | Purpose | Trigger |
|---|---|---|---|
| Unit/contract | No JVM, fake Connect responses | Logic and semantics | Every PR update |
| Packaging | Fresh wheel environments | Optional dependency boundaries | Every PR update |
| Classic integration | Local Spark 3.5 and selected 4.x with compatible Java | Real logs, AQE on/off, lifecycle | CI |
| Serverless smoke | Databricks `--profile free` | Hook/API compatibility and actual metric shapes | Manual, one bounded job |
| Managed classic | Databricks Standard and Dedicated access modes | Permissions, logging and session behavior | Explicit follow-up environment |

Free Edition only provides serverless compute, so it cannot validate the managed
classic lane. [Databricks Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations).

For the next live run:

1. Verify `--profile free` authentication; reauthenticate only if expired. The user
   refreshed it during this review and job listing succeeded. Inspect existing jobs once.
   Do not launch the current broad demo/debug sweep automatically.
2. Build one wheel and deploy one small smoke notebook. Set maximum concurrent runs
   to one, timeout to ten minutes, retries to zero. Use `spark.range` inputs capped
   at 1,000 rows, a groupBy, two joins with different keys, repeated actions, and one
   intentional exception. Avoid full sample-table scans or forced spill workloads.
3. Run once; poll no more often than every 30 seconds. Stop on quota/rate-limit
   responses and record retry guidance; no immediate job resubmission. Cancel only
   the test's own run if it exceeds the budget.
4. Assert public capture outputs, query attribution, unknown handling with offline
   injection, and session usability after exit. Save sanitized raw metric fixtures,
   versions, action coverage, and outcomes; replay offline thereafter. Clean only
   artifacts owned by this test. No runtime performance thresholds across compute types.

Observed this review: authentication initially failed, then worked after the user
refreshed it. Existing jobs allow four concurrent runs and have no timeout; do not
inherit those settings for quota-conscious tests. No remote workload was started.
The smoke procedure is a future acceptance test, not evidence that live compatibility passes.
