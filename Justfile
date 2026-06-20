ci:
    uv run ruff check sparkparse/ tests/
    uv run ruff format --check sparkparse/ tests/
    uv run pyrefly check sparkparse/ tests/
    uv run pytest tests/ --ignore=tests/test.py --ignore=tests/test_capture.py -v

# Trigger the test workflow on the current branch and watch it run.
# The branch must be pushed to GitHub first (workflow_dispatch needs a remote ref).
# Requires gh auth login.
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
