import logging
from pathlib import Path

import typer

from sparkparse.common import write_dataframe
from sparkparse.models import OutputFormat
from sparkparse.parse import log_to_df, parse_log

app = typer.Typer(pretty_exceptions_enable=False)


@app.command()
def get(
    base_dir: str = "data",
    log_dir: str = "logs/raw",
    output_dir: str = "logs/parsed",
    log_file: str | None = None,
    out_format: OutputFormat = OutputFormat.csv,
    verbose: bool = False,
):
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

    result = parse_log(log_to_parse)
    df = log_to_df(result)

    out_dir = base_dir_path / output_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"{log_to_parse.stem}.csv"

    logging.info(f"Writing parsed log: {out_path}")
    logging.debug(f"Output format: {out_format}")
    logging.debug(f"{df.shape[0]} rows and {df.shape[1]} columns")
    logging.debug(f"{df.head()}")

    write_dataframe(df, out_path, out_format)


@app.command()
def welcome():
    typer.echo("Welcome to sparkparse CLI")
    typer.echo("Use --help to see available commands")


if __name__ == "__main__":
    app()
