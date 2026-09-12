# Project review and implementation queue

Reviewed 2026-09-12 at commit `129318c`. Brief 01 is implemented and serverless-smoke
validated; the remaining briefs are proposals.
This queue supersedes the pending-work descriptions in `IMPLEMENTATION.md` and
`plans/SPARK_CONNECT_CAPTURE.md`; retain those documents as historical context.

## Recommendation

Prioritize trustworthy standard/serverless parity before adding more heuristics.
The project already has useful parsing, plan visualization, analysis helpers,
cloud I/O, history, and alerts. The largest problem is the contract between those
features: the Connect adapter supplies only plan data, while downstream consumers
often assume task data exists or interpret its absence as zero.

Here, “standard” means classic Spark/Databricks compute. Databricks Standard access
mode is a separate compatibility dimension; test it separately from Dedicated
access mode. Spark Connect is a transport, not proof that compute is serverless.

## Implementation order

| Priority | Brief | Outcome | Relative size / dependency |
|---|---|---|---|
| P0 | [01 — Capture and capabilities](01-capture-and-capabilities.md) | Safe session lifecycle, common finalization, explicit missing data | Implemented + smoke-tested |
| P0 | [02 — Connect correctness](02-connect-correctness.md) | Reliable query attribution and tolerant operator handling | Implemented offline; live validation outstanding |
| P0 | [03 — Analysis correctness and depth](03-analysis.md) | Accurate metrics and evidence-based findings | Implemented offline; increments 1–3 |
| P1 | [04 — Event-log robustness](04-event-logs.md) | Non-AQE, partial, rolled, retried, and larger workloads | Large; 01 identities |
| P1 | [05 — Developer experience and validation](05-developer-experience.md) | Installable CLI, reproducible checks, serverless-friendly reports | Medium; packaging can start immediately |
| P1 | [06 — History and comparisons](06-history.md) | Comparable runs and meaningful regression alerts | Medium; 01 and 03 |

Split each brief into its listed reviewable increments. Start with dependency/CLI
fixes from 05 and the capability contract from 01, then complete capture and metric
correctness before adding analytical rules. Do not implement every brief in one PR.

## Current parity, established by source review

| Feature | Classic path today | Connect path today | Target |
|---|---|---|---|
| Session lifecycle | Stops/recreates caller session | Patches private client methods | Borrowed sessions remain usable |
| DAG and operator metrics | Event-log parsing | PlanMetrics conversion | Shared versioned semantics; source retained |
| Unknown operators | Tolerant fallback unless strict | Raises at conversion | Preserve unknown nodes; optional strict failure |
| `action="analyze"` | Builds summary | Early return bypasses summary | Shared finalizer |
| History / alerts | Implemented for parsed actions | Early return bypasses both | Shared finalizer with coverage checks |
| Spill analysis | Task metrics | Returns empty even with plan spill | Operator-level evidence where available |
| Largest scans | Task attribution | No plan-metric fallback | Rank observed bytes/rows separately |
| Task skew / stage timeline | Available when event data supports it | Empty task frame | Explicit unsupported status |
| Query elapsed time | Event timestamps | Root cumulative operator time | Distinguish elapsed from aggregate time |
| Dashboard | Raw-log directory oriented | Standalone DAG use is possible | Load a persisted capture artifact |

Full task telemetry parity is not a realistic baseline for the current Connect
source. Databricks documents unavailable Spark UI and Spark logs on serverless;
query profiles are the supported UI alternative. An optional profile importer may
be investigated, but should not become a mandatory account-level dependency.
[Databricks serverless limitations](https://docs.databricks.com/aws/en/compute/serverless/limitations).

## Validation performed

Used existing `.venv` tools, without dependency upgrades:

- `.venv/bin/python -m pytest tests/ --ignore=tests/test.py --ignore=tests/test_capture.py -q`:
  **233 passed, 2 skipped**, five `melt` deprecation warnings.
- `.venv/bin/ruff check sparkparse/ tests/`: passed.
- `.venv/bin/ruff format --check sparkparse/ tests/`: 33 files already formatted.
- `.venv/bin/pyrefly check sparkparse/ tests/`: two errors:
  `pages/home.py:132` (`Timedelta.dt`) and `pages/summary.py:172`
  (`Series.to_dict("records")` overload). Investigate runtime behavior before fixing.
- In-memory Connect probe with a `PhotonGroupingAgg` carrying
  `numBytesSpilled=1024`: `find_spill` returned zero findings and summary task totals
  were all zero. No Spark execution was needed.
- `_get_output_rows` with a measured classic metric of zero returned `None`.
- An unknown Connect operator raised `ValueError` during DataFrame conversion.
- No dedicated Connect tests found in the current Python test sources.

Live validation was attempted using CLI v1.15.0 and `--profile free`. The first call
reported old credential-cache storage; a process-local `DATABRICKS_AUTH_STORAGE=plaintext`
retry reached authentication after resolving sandbox networking and reported an
invalid refresh token. The user subsequently reauthenticated; a normal
`databricks jobs list --profile free --output json` succeeded. Both existing jobs
declare environment client `2`, maximum concurrent runs `4`, and timeout `0`.
Latest-run lookups for both jobs returned no retained runs.
**No remote query/job was started and no live execution parity was established.**
Authentication is now working; use the bounded smoke test in 05 for execution validation.
No full local JVM integration suite or clean-wheel install was run in this review.

## Handoff instructions

Read this index and the selected brief, inspect current code, and implement only
that increment. Treat proposed APIs as design targets, not existing functionality.
Keep compatibility shims where described, add adversarial fixtures, and report
offline and live validation separately. Do not treat old roadmap claims as tests:
the old Connect brief claims plan-based spill/scan support that current code lacks.
