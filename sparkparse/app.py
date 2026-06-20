import json
import sys
from enum import StrEnum
from pathlib import Path
from typing import Annotated

import typer

from sparkparse.analyze import to_plan_summary
from sparkparse.dashboard import init_dashboard, run_app
from sparkparse.models import OutputFormat, ParsedLogDataFrames
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
) -> ParsedLogDataFrames:
    """Parse Spark event logs and write structured DataFrames to disk."""
    return get_parsed_metrics(
        log_dir=log_dir,
        log_file=log_file,
        out_dir=out_dir,
        out_name=out_name,
        out_format=out_format,
        verbose=verbose,
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
        typer.Option(help="Write analysis output to this file (local path or cloud URI)."),
    ] = None,
    format: Annotated[
        AnalysisFormat,
        typer.Option(help="Output format: 'json' (default) or human-readable 'text'."),
    ] = AnalysisFormat.json,
) -> None:
    """Analyze Spark event logs and emit a token-efficient summary suitable for LLM piping."""
    dfs = get_parsed_metrics(
        log_dir=log_dir,
        log_file=log_file,
        out_dir=None,
        out_name=None,
        out_format=None,
        verbose=False,
    )

    log_name = get_path_stem(log_file) if log_file else get_path_name(log_dir)
    summary = to_plan_summary(dfs, log_name)

    if format == AnalysisFormat.json:
        output = json.dumps(summary, indent=2, default=str)
    else:
        lines: list[str] = [f"Log: {summary['log_name']}"]
        totals = summary.get("totals", {})
        lines.append(f"Queries: {len(summary.get('queries', []))}")
        lines.append(f"Bytes read: {totals.get('bytes_read', 0):,}")
        lines.append(f"Bytes written: {totals.get('bytes_written', 0):,}")
        lines.append(f"Shuffle bytes read: {totals.get('shuffle_bytes_read', 0):,}")
        lines.append(f"Shuffle bytes written: {totals.get('shuffle_bytes_written', 0):,}")
        lines.append(f"Memory spilled: {totals.get('memory_bytes_spilled', 0):,}")
        lines.append(f"Disk spilled: {totals.get('disk_bytes_spilled', 0):,}")
        for q in summary.get("queries", []):
            lines.append(
                f"\nQuery {q['query_id']} ({q['query_function']})"
                f"  duration={q['duration_seconds']:.1f}s"
                f"  nodes={len(q['nodes'])}"
            )
            for n in q["nodes"]:
                dur = (
                    f"{n['duration_minutes']:.3f}min"
                    if n.get("duration_minutes") is not None
                    else "?"
                )
                lines.append(f"  [{n['node_id']}] {n['node_type']}  {dur}")
        output = "\n".join(lines)

    if out_file is not None:
        if not is_cloud_path(out_file):
            Path(out_file).parent.mkdir(parents=True, exist_ok=True)
        write_text(out_file, output)
        typer.echo(f"Analysis written to {out_file}", err=True)
    else:
        sys.stdout.write(output + "\n")


if __name__ == "__main__":
    app()
