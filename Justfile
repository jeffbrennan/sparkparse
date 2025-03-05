jsonformat:
    mkdir -p data/logs/sandbox
    for file in data/logs/raw/*; do \
        base=$(basename "$file"); \
        jq '.' "$file" > "data/logs/sandbox/${base}.json"; \
    done