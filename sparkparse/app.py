import typer

from sparkparse.dashboard import init_dashboard, run_app
from sparkparse.models import OutputFormat, ParsedLogDataFrames
from sparkparse.parse import get_parsed_metrics

app = typer.Typer(pretty_exceptions_enable=False)


@app.command("viz")
def viz_parsed_metrics(
    log_dir: str = "data/logs/raw",
    force_port: bool = typer.Option(
        default=False, help="force kill processes using port 8050"
    ),
) -> None:
    app = init_dashboard(log_dir)
    run_app(app=app, force_port=force_port)


@app.command("get")
def get(
    log_dir: str = "data/logs/raw",
    log_file: str | None = None,
    out_dir: str | None = "data/logs/parsed",
    out_name: str | None = None,
    out_format: OutputFormat | None = OutputFormat.csv,
    verbose: bool = False,
) -> ParsedLogDataFrames:
    return get_parsed_metrics(
        log_dir=log_dir,
        log_file=log_file,
        out_dir=out_dir,
        out_name=out_name,
        out_format=out_format,
        verbose=verbose,
    )


@app.command()
def welcome():
    typer.echo("Welcome to sparkparse CLI")
    typer.echo("Use --help to see available commands")


if __name__ == "__main__":
    app()
