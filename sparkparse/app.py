import json
import sys
from enum import StrEnum
from pathlib import Path
from typing import Annotated

import typer

from sparkparse import alerts, history
from sparkparse.analyze import to_analysis_export, to_plan_summary
from sparkparse.dashboard import init_dashboard, run_app
from sparkparse.models import OutputFormat, ParsedLogDataFrames, RunRecord
from sparkparse.parse import get_parsed_metrics
from sparkparse.storage import (
    get_path_name,
    get_path_stem,
    is_cloud_path,
    write_text,
)

__version__ = "0.1.0"

app = typer.Typer(pretty_exceptions_enable=False)


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
    force_port: bool = typer.Option(
        default=False, help="Force kill any process using port 8050 before starting."
    ),
) -> None:
    """Launch the interactive Dash dashboard for a directory of Spark event logs."""
    app = init_dashboard(log_dir)
    run_app(app=app, force_port=force_port)


@app.command("get")
def get(
    log_dir: Annotated[
        str, typer.Argument(help="Directory containing raw Spark event logs.")
    ] = "data/logs/raw",
    log_file: Annotated[
        str | None,
        typer.Option(help="Parse a single log file instead of the whole directory."),
    ] = None,
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
) -> ParsedLogDataFrames:
    """Parse Spark event logs and write structured DataFrames to disk."""
    return get_parsed_metrics(
        log_dir=log_dir,
        log_file=log_file,
        out_dir=out_dir,
        out_name=out_name,
        out_format=out_format,
        verbose=verbose,
        strict=strict,
    )


@app.command("analyze")
def analyze(
    log_dir: Annotated[
        str, typer.Argument(help="Directory containing raw Spark event logs.")
    ] = "data/logs/raw",
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
    latest = RunRecord(**latest_row)

    rules = alerts.load_alert_config(alert_config)
    triggered = alerts.check_alerts(latest, hist_df, rules, alert_output_path)

    typer.echo(json.dumps(triggered, indent=2, default=str))


if __name__ == "__main__":
    app()
