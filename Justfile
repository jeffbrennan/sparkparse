export JAVA_HOME := `/usr/libexec/java_home -v 17 2>/dev/null || echo /opt/homebrew/opt/openjdk@17`

ci:
    uv run ruff check sparkparse/ tests/
    uv run ruff format --check sparkparse/ tests/
    uv run pyrefly check sparkparse/ tests/
    uv run pytest tests/ --ignore=tests/test.py --ignore=tests/test_capture.py -v

# Trigger the test workflow on the current branch and watch it run.
# The workflow also auto-runs when a PR is opened/reopened; use this for
# manual re-runs after pushing new commits. Requires gh auth login.
run-ci:
    @branch=$(git branch --show-current); \
    echo "Triggering test workflow on branch: $branch"; \
    gh workflow run test.yml --ref $branch || { echo "Failed to trigger workflow. Is the branch pushed to GitHub?"; exit 1; }; \
    sleep 3; \
    run_id=$(gh run list --workflow=test.yml --branch=$branch --limit=1 --json databaseId --jq '.[0].databaseId'); \
    if [ -z "$run_id" ]; then echo "Could not find the triggered run. Check: gh run list --workflow=test.yml"; exit 1; fi; \
    echo "Watching run $run_id..."; \
    gh run watch $run_id --exit-status && echo "CI passed" || { echo "CI failed"; gh run view $run_id --log-failed; exit 1; }

jsonformat:
    mkdir -p data/logs/sandbox
    for file in data/logs/raw/*; do \
        base=$(basename "$file"); \
        jq '.' "$file" > "data/logs/sandbox/${base}.json"; \
    done

live:
    open http://localhost:4040/

history:
    ($SPARK_HOME/sbin/start-history-server.sh || open http://localhost:18080/)

# Generate synthetic parquet files used by Spark integration tests.
# Produces data/raw/G1_1e7_1e7_100_0.parquet (small, ~0.5 GB) and
# data/raw/G1_1e8_1e8_100_0.parquet (medium, ~5 GB). Large (50 GB) is
# omitted — only test_complex_transformation needs medium, most tests use small.
gen-data:
    mkdir -p data/raw
    uv run falsa groupby --path-prefix data/raw --size SMALL  --k 100 --nas 0 --data-format PARQUET
    uv run falsa groupby --path-prefix data/raw --size MEDIUM --k 100 --nas 0 --data-format PARQUET

# Delete the generated parquet files (leaves other data/raw/ output dirs intact).
clean-data:
    rm -f data/raw/G1_*.parquet
