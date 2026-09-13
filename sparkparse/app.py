import importlib.metadata
import json
import sys
from enum import StrEnum
from pathlib import Path
from typing import Annotated

import polars as pl
import typer

from sparkparse import alerts, history
from sparkparse.analyze import to_analysis_export, to_plan_summary
from sparkparse.artifact import save_capture_artifact
from sparkparse.eventlog import discover_sources
from sparkparse.models import OutputFormat, ParsedLogDataFrames
from sparkparse.parse import get_all_parsed_metrics, get_parsed_metrics
from sparkparse.storage import (
    get_path_name,
    get_path_stem,
    is_cloud_path,
    write_text,
)


def _package_version() -> str:
    try:
        return importlib.metadata.version("sparkparse")
    except importlib.metadata.PackageNotFoundError:
        return "0.0.0"


__version__ = _package_version()

app = typer.Typer(pretty_exceptions_enable=False)


def _resolve_log_dir(
    log_dir: str, log_dir_option: str | None, *, require_exists: bool = True
) -> str:
    """Accept ``--log-dir`` as an alias for the positional log directory.

    The positional argument is canonical; the option exists only so the
    previously documented ``sparkparse get --log-dir ./logs`` keeps working.
    """
    if log_dir_option is not None:
        if log_dir != "data/logs/raw":
            raise typer.BadParameter(
                "Pass the log directory either as the argument or --log-dir, not both."
            )
        log_dir = log_dir_option
    if require_exists and not is_cloud_path(log_dir) and not Path(log_dir).exists():
        raise typer.BadParameter(f"Log directory does not exist: {log_dir}")
    return log_dir


class AnalysisFormat(StrEnum):
    json = "json"
    text = "text"


class HistoryFormat(StrEnum):
    table = "table"
    json = "json"


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"sparkparse {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: Annotated[
        bool | None,
        typer.Option(
            "--version",
            callback=_version_callback,
            is_eager=True,
            help="Show version and exit.",
        ),
    ] = None,
) -> None:
    pass


@app.command("viz")
def viz_parsed_metrics(
    log_dir: Annotated[
        str, typer.Argument(help="Directory containing raw Spark event logs.")
    ] = "data/logs/raw",
    log_dir_option: Annotated[
        str | None,
        typer.Option("--log-dir", help="Alias for the log-dir argument."),
    ] = None,
    force_port: bool = typer.Option(
        default=False, help="Force kill any process using port 8050 before starting."
    ),
) -> None:
    """Launch the interactive Dash dashboard for a directory of Spark event logs."""
    log_dir = _resolve_log_dir(log_dir, log_dir_option)
    try:
        from sparkparse.dashboard import init_dashboard, run_app
    except ImportError as exc:  # pragma: no cover - exercised only without [viz]
        raise typer.BadParameter(
            "The dashboard requires the 'viz' extra: install with "
            "`pip install sparkparse[viz]`."
        ) from exc

    app = init_dashboard(log_dir)
    run_app(app=app, force_port=force_port)


@app.command("get")
def get(
    log_dir: Annotated[
        str, typer.Argument(help="Directory containing raw Spark event logs.")
    ] = "data/logs/raw",
    log_dir_option: Annotated[
        str | None,
        typer.Option("--log-dir", help="Alias for the log-dir argument."),
    ] = None,
    log_file: Annotated[
        str | None,
        typer.Option(
            help="Event log to parse: file name, rolling-log directory, "
            "application id, or full path. Default: the newest source in "
            "log_dir (most recent mtime, name order when there is none)."
        ),
    ] = None,
    all_apps: Annotated[
        bool,
        typer.Option(
            "--all-apps",
            help="Parse every application in log_dir instead of just the newest.",
        ),
    ] = False,
    out_dir: Annotated[
        str | None, typer.Option(help="Directory to write parsed output files.")
    ] = "data/logs/parsed",
    out_name: Annotated[
        str | None,
        typer.Option(help="Base name for output files (defaults to log file stem)."),
    ] = None,
    out_format: Annotated[
        OutputFormat | None, typer.Option(help="Output file format.")
    ] = OutputFormat.csv,
    verbose: Annotated[bool, typer.Option(help="Print extra parsing details.")] = False,
    strict: Annotated[
        bool,
        typer.Option(help="Hard-fail on unrecognized node types or detail models."),
    ] = False,
    artifact: Annotated[
        str | None,
        typer.Option(
            "--artifact",
            help="Write a portable capture artifact directory that the dashboard "
            "can open without the raw event logs.",
        ),
    ] = None,
) -> ParsedLogDataFrames:
    """Parse Spark event logs and write structured DataFrames to disk."""
    log_dir = _resolve_log_dir(log_dir, log_dir_option, require_exists=log_file is None)
    if artifact is not None and all_apps:
        raise typer.BadParameter("--artifact cannot be combined with --all-apps")
    if all_apps:
        if log_file is not None:
            raise typer.BadParameter("--all-apps cannot be combined with --log-file")
        results = get_all_parsed_metrics(
            log_dir=log_dir,
            out_dir=out_dir,
            out_format=out_format,
            verbose=verbose,
            strict=strict,
        )
        # Query ids restart per application, so the results stay separate. The
        # command returns the newest one for interactive use; every one of them
        # is written to out_dir.
        typer.echo(f"Parsed {len(results)} application(s): {', '.join(results)}")
        return results[sorted(results)[-1]]

    result = get_parsed_metrics(
        log_dir=log_dir,
        log_file=log_file,
        out_dir=out_dir,
        out_name=out_name,
        out_format=out_format,
        verbose=verbose,
        strict=strict,
    )
    if artifact is not None:
        label = out_name or (get_path_stem(log_file) if log_file else None)
        path = save_capture_artifact(result, artifact, label=label)
        typer.echo(f"Capture artifact written to {path}", err=True)
    return result


@app.command("logs")
def logs(
    log_dir: Annotated[
        str, typer.Argument(help="Directory containing raw Spark event logs.")
    ] = "data/logs/raw",
    log_dir_option: Annotated[
        str | None,
        typer.Option("--log-dir", help="Alias for the log-dir argument."),
    ] = None,
    format: Annotated[
        AnalysisFormat,
        typer.Option(help="Output format: 'text' (default) or 'json'."),
    ] = AnalysisFormat.text,
) -> None:
    """List the event-log sources discovered in a directory.

    Rolling logs collapse into one source with ordered segments; marker files
    and checksums are ignored. The last row is the one a bare parse selects.
    """
    log_dir = _resolve_log_dir(log_dir, log_dir_option)
    sources = discover_sources(log_dir)
    if not sources:
        typer.echo(f"No event log sources found in {log_dir}")
        raise typer.Exit(1)

    ordered = sorted(sources, key=lambda item: (item.modified or 0, item.name))
    if format == AnalysisFormat.json:
        typer.echo(json.dumps([s.model_dump(mode="json") for s in ordered], indent=2))
        return

    for source in ordered:
        codecs = ",".join(codec.value for codec in source.codecs)
        typer.echo(
            f"{source.name}\t"
            f"segments={len(source.segments)}\t"
            f"rolling={source.rolling}\t"
            f"complete={source.complete}\t"
            f"codec={codecs}"
        )
    typer.echo(f"\nNewest (default selection): {ordered[-1].name}", err=True)


@app.command("analyze")
def analyze(
    log_dir: Annotated[
        str, typer.Argument(help="Directory containing raw Spark event logs.")
    ] = "data/logs/raw",
    log_dir_option: Annotated[
        str | None,
        typer.Option("--log-dir", help="Alias for the log-dir argument."),
    ] = None,
    log_file: Annotated[
        str | None,
        typer.Option(help="Analyze a single log file instead of the whole directory."),
    ] = None,
    out_file: Annotated[
        str | None,
        typer.Option(
            help="Write analysis output to this file (local path or cloud URI)."
        ),
    ] = None,
    format: Annotated[
        AnalysisFormat,
        typer.Option(help="Output format: 'json' (default) or human-readable 'text'."),
    ] = AnalysisFormat.json,
    compact: Annotated[
        bool,
        typer.Option(
            help="Drop display-only fields and keep only the longest-running nodes."
        ),
    ] = False,
    top_n: Annotated[
        int | None,
        typer.Option(help="Nodes to keep per query (default: all; 25 with --compact)."),
    ] = None,
    redact: Annotated[
        bool,
        typer.Option(help="Replace file paths and expressions with hashed tokens."),
    ] = False,
    findings: Annotated[
        bool,
        typer.Option(
            help="Include diagnostic findings and per-rule status alongside the "
            "raw plan summary."
        ),
    ] = True,
) -> None:
    """Analyze Spark event logs and emit a token-efficient summary suitable for LLM piping.

    The raw plan summary and the diagnostic findings stay separate: the summary
    states measured facts, the findings interpret them and carry their own
    evidence, thresholds and coverage status.
    """
    log_dir = _resolve_log_dir(log_dir, log_dir_option, require_exists=log_file is None)
    dfs = get_parsed_metrics(
        log_dir=log_dir,
        log_file=log_file,
        out_dir=None,
        out_name=None,
        out_format=None,
        verbose=False,
    )

    log_name = get_path_stem(log_file) if log_file else get_path_name(log_dir)
    summary = to_plan_summary(
        dfs, log_name, compact=compact, top_n=top_n, redact=redact
    )
    analysis = to_analysis_export(dfs, log_name, redact=redact) if findings else None

    if format == AnalysisFormat.json:
        payload = dict(summary)
        if analysis is not None:
            payload["analysis"] = analysis
        output = json.dumps(payload, indent=2, default=str)
    else:
        totals = summary.get("totals", {})

        def total(name: str) -> str:
            # None means the source never reported the metric, not zero work.
            value = totals.get(name)
            return "not available" if value is None else f"{value:,}"

        lines: list[str] = [f"Log: {summary['log_name']}"]
        lines.append(f"Queries: {len(summary.get('queries', []))}")
        lines.append(f"Bytes read: {total('bytes_read')}")
        lines.append(f"Bytes written: {total('bytes_written')}")
        lines.append(f"Shuffle bytes read: {total('shuffle_bytes_read')}")
        lines.append(f"Shuffle bytes written: {total('shuffle_bytes_written')}")
        lines.append(f"Memory spilled: {total('memory_bytes_spilled')}")
        lines.append(f"Disk spilled: {total('disk_bytes_spilled')}")
        for q in summary.get("queries", []):
            duration = (
                f"{q['duration_seconds']:.1f}s"
                if q.get("duration_seconds") is not None
                else "not available"
            )
            omitted = (
                f"  omitted={q['omitted_node_count']}"
                if q.get("omitted_node_count")
                else ""
            )
            lines.append(
                f"\nQuery {q['query_id']} ({q['query_function']})"
                f"  duration={duration}"
                f"  nodes={len(q['nodes'])}{omitted}"
            )
            for n in q["nodes"]:
                dur = (
                    f"{n['duration_minutes']:.3f}min"
                    if n.get("duration_minutes") is not None
                    else "?"
                )
                lines.append(f"  [{n['node_id']}] {n['node_type']}  {dur}")
        if analysis is not None:
            lines.append("")
            lines.append(f"Findings ({len(analysis['findings'])}):")
            for finding in analysis["findings"]:
                scope = []
                if finding["query_id"] is not None:
                    scope.append(f"q{finding['query_id']}")
                if finding["stage_id"] is not None:
                    scope.append(f"s{finding['stage_id']}")
                scope_str = f" [{'/'.join(scope)}]" if scope else ""
                lines.append(
                    f"  [{finding['severity']}] {finding['category']}{scope_str}"
                    f" ({finding['confidence']} confidence): {finding['observation']}"
                )
            skipped = [
                assessment
                for assessment in analysis["assessments"]
                if assessment["status"] != "evaluated"
            ]
            if skipped:
                lines.append("")
                lines.append("Rules not evaluated:")
                for assessment in skipped:
                    lines.append(
                        f"  {assessment['rule_id']}: {assessment['status']}"
                        f" — {assessment['reason']}"
                    )
        output = "\n".join(lines)

    if out_file is not None:
        if not is_cloud_path(out_file):
            Path(out_file).parent.mkdir(parents=True, exist_ok=True)
        write_text(out_file, output)
        typer.echo(f"Analysis written to {out_file}", err=True)
    else:
        sys.stdout.write(output + "\n")


@app.command("history")
def history_cmd(
    history_path: Annotated[
        str, typer.Argument(help="Path to history store (Delta dir or JSONL file).")
    ],
    log_name: Annotated[
        str | None,
        typer.Option(help="Filter records to this log_name only."),
    ] = None,
    last: Annotated[
        int | None, typer.Option(help="Show only the last N runs (most recent first).")
    ] = None,
    format: Annotated[
        HistoryFormat,
        typer.Option(help="Output format: 'table' (default) or 'json'."),
    ] = HistoryFormat.table,
) -> None:
    """Query the run history store for past job metrics."""
    df = history.read(history_path, log_name=log_name, last_n=last)

    if df.is_empty():
        typer.echo("No history records found.")
        return

    if format == HistoryFormat.json:
        typer.echo(json.dumps(df.to_dicts(), default=str, indent=2))
    else:
        typer.echo(str(df))


@app.command("check-alerts")
def check_alerts_cmd(
    history_path: Annotated[
        str, typer.Argument(help="Path to history store (Delta dir or JSONL file).")
    ],
    log_name: Annotated[
        str, typer.Argument(help="Stable job identifier to check alerts for.")
    ],
    alert_config: Annotated[
        str, typer.Argument(help="Path to alert configuration TOML file.")
    ],
    alert_output_path: Annotated[
        str | None,
        typer.Option(
            help="File to write triggered alerts (for on_trigger='file' rules)."
        ),
    ] = None,
) -> None:
    """Run alert checks against the latest run in history."""
    hist_df = history.read(history_path, log_name=log_name)

    if hist_df.is_empty():
        typer.echo(f"No history records found for log_name '{log_name}'.", err=True)
        raise typer.Exit(1)

    latest_row = hist_df.sort("run_at").row(-1, named=True)
    latest = history.row_to_record(latest_row)

    rules = alerts.load_alert_config(alert_config)
    assessments = alerts.check_alerts(latest, hist_df, rules, alert_output_path)

    typer.echo(
        json.dumps(
            [assessment.model_dump(mode="json") for assessment in assessments],
            indent=2,
            default=str,
        )
    )


@app.command("compare")
def compare_cmd(
    history_path: Annotated[
        str, typer.Argument(help="Path to history store (Delta dir or JSONL file).")
    ],
    log_name: Annotated[
        str, typer.Option(help="Workload identifier to compare the latest run for.")
    ],
    window: Annotated[
        int, typer.Option(help="Maximum baseline samples per metric.")
    ] = 10,
    min_samples: Annotated[
        int, typer.Option(help="Minimum comparable baseline samples per metric.")
    ] = 1,
    match_fingerprint: Annotated[
        bool, typer.Option(help="Restrict the cohort to the same plan fingerprint.")
    ] = True,
    match_backend: Annotated[
        bool, typer.Option(help="Restrict the cohort to the same backend.")
    ] = False,
    match_runtime: Annotated[
        bool, typer.Option(help="Restrict the cohort to the same runtime version.")
    ] = False,
    format: Annotated[
        HistoryFormat,
        typer.Option(help="Output format: 'table' (default) or 'json'."),
    ] = HistoryFormat.table,
) -> None:
    """Compare the latest run against comparable history."""
    try:
        report = history.compare_runs(
            history_path,
            log_name,
            window=window,
            min_samples=min_samples,
            match_fingerprint=match_fingerprint,
            match_backend=match_backend,
            match_runtime=match_runtime,
        )
    except ValueError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc

    if format == HistoryFormat.json:
        typer.echo(report.model_dump_json(indent=2))
        return

    typer.echo(f"Workload: {report.log_name}")
    typer.echo(f"Current run: {report.current_run_id} at {report.current_run_at}")
    typer.echo(
        f"Cohort: {report.cohort_size} sample(s) "
        f"(window={report.window}), plan_changed={report.plan_changed}"
    )
    if report.metrics:
        rows = pl.DataFrame(
            [
                {
                    "metric": m.metric,
                    "current": m.current,
                    "baseline": m.baseline,
                    "delta": m.delta,
                    "pct_change": m.pct_change,
                    "samples": m.sample_count,
                }
                for m in report.metrics
            ]
        )
        typer.echo(str(rows))
    else:
        typer.echo("No comparable metrics.")
    for reason in report.excluded:
        typer.echo(f"  excluded: {reason}")


if __name__ == "__main__":
    app()
