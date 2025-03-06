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