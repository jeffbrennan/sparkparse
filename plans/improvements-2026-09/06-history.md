# 06 — Comparable history and trustworthy regression alerts

Status: proposed. Priority: P1. Depends on 01 and 03. Files: `history.py`,
`alerts.py`, `models.py`, capture finalizer, and CLI.

## Evidence

`record_from_dfs` derives elapsed time from DAG timestamps and substitutes zero when
they are absent, as on Connect. Missing scan/task totals are also coerced to zero.
It combines memory/disk spill into one value, and baselines group only by log name.
`_compute_baseline` returns zero when no history exists, allowing a first positive
run to trigger a percent-increase alert. History auto-write chooses format from
installed packages, while auto-read also checks `.jsonl`; installing Delta can change
behavior for the same path. The capture API has no alert-output-path forwarding for
file dispatch. File append semantics also need a concurrency policy for cloud stores.

## Design

Version RunRecord and include coverage, backend, runtime/client version, workload
fingerprint, relevant configuration, input-size evidence, and execution status.
Use null for absent measures. Preserve elapsed/cumulative timing distinctions and
separate memory/disk spill definitions. Read old records as legacy with unknown
provenance; never reinterpret historical zeros as known missing measurements.

Default baseline cohorts require the same workload identity and comparable metric
semantics/coverage. Runtime/compute differences should be visible and selectable;
cross-backend comparison is allowed explicitly for compatible measures. Normalized
measures (for example, seconds per observed input row) require a measured denominator
and do not remove cache/warmup/data-shape confounding. Add before/after plan diffs with
stable fingerprints rather than unstable node IDs as a later increment.

Require a configurable minimum valid baseline sample count. Return `insufficient_data`
when absent; absolute thresholds may still evaluate a known current value. Handle
true zero baselines explicitly, exclude current run and later runs in historical
evaluation, and validate metric names, positive windows, and finite thresholds.
Unavailable current metrics produce skipped assessments. Preserve per-rule outcomes
even when no alerts fire. Forward file output path from capture.

Make store format explicit or persist a format marker and inspect existing storage
before choosing. A `.jsonl` path must stay JSONL after Delta installation. For multiple
writers use Delta transactions or immutable per-run objects plus a manifest strategy;
do not promise atomic object-store append. Idempotency is based on stable run ID.

## Increments and acceptance

1. Versioned nullable records, migration reader, deterministic storage selection.
   Round-trip classic, Connect, legacy, empty, and partial runs. Installing an optional
   package cannot silently change an existing store format.
2. Baseline selection and rule assessments. First run, insufficient samples, null/zero,
   differing metric semantics, changed input size, out-of-order history, duplicate run,
   and file-dispatch tests. No false improvement caused by unavailable telemetry.
3. Comparison command/report: show compatible metrics, excluded measures and reasons,
   cohort/sample count, and optional plan changes. Test equivalent plans with changed
   node IDs and meaningful join/scan changes. Concurrent append tests match the chosen
   storage guarantee; document unsupported writer modes rather than silently losing runs.
