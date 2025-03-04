from pathlib import Path

from sparkparse.parse import log_to_df, parse_log


def main():
    print("running sparkparse!")
    base_dir = Path(__file__).parents[1] / "data"
    log_dir = base_dir / "logs" / "raw"
    latest_log = sorted(log_dir.glob("*"))[-1]

    result = parse_log(latest_log)
    df = log_to_df(result)
    print(df.head())

    df.write_csv(f"data/logs/parsed/{latest_log.stem}.csv", include_header=True)


if __name__ == "__main__":
    main()
