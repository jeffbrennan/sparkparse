import logging
from pathlib import Path

import polars as pl
import typer

from sparkparse.dashboard import init_dashboard, run_app
from sparkparse.models import OutputFormat
from sparkparse.parse import log_to_df, parse_log, write_parsed_log

app = typer.Typer(pretty_exceptions_enable=False)


@app.command("viz")
def viz_parsed_metrics(
    base_dir: str = "data",
    log_dir: str = "logs/raw",
    log_file: str | None = None,
    out_dir: str | None = "logs/parsed",
    out_name: str | None = None,
    out_format: OutputFormat | None = OutputFormat.csv,
    verbose: bool = False,
) -> None:
    df = get_parsed_metrics(
        base_dir=base_dir,
        log_dir=log_dir,
        log_file=log_file,
        out_dir=out_dir,
        out_name=out_name,
        out_format=out_format,
        verbose=verbose,
    )
    app = init_dashboard(df)
    run_app(app)


@app.command("get")
def get_parsed_metrics(
    base_dir: str = "data",
    log_dir: str = "logs/raw",
    log_file: str | None = None,
    out_dir: str | None = "logs/parsed",
    out_name: str | None = None,
    out_format: OutputFormat | None = OutputFormat.csv,
    verbose: bool = False,
) -> pl.DataFrame:
    base_dir_path = Path(__file__).parents[1] / base_dir
    log_dir_path = base_dir_path / log_dir

    if verbose:
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s.%(msecs)03d %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
    else:
        logging.basicConfig(level=logging.INFO, format="%(message)s")

    if log_file is None:
        log_to_parse = sorted(log_dir_path.glob("*"))[-1]
    else:
        log_to_parse = log_dir_path / log_file

    logging.info(f"Reading log file: {log_to_parse}")

    result = parse_log(log_to_parse, out_name)
    df = log_to_df(result, log_to_parse.stem)

    if out_dir is None or out_format is None:
        logging.info("Skipping writing parsed log")
        return df

    write_parsed_log(df, base_dir_path, out_dir, out_format, result.name)
    return df


@app.command()
def welcome():
    typer.echo("Welcome to sparkparse CLI")
    typer.echo("Use --help to see available commands")


if __name__ == "__main__":
    app()
