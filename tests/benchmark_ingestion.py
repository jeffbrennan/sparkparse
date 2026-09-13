"""Measure event-log ingestion cost: elapsed time and process peak RSS.

Run as a script so the reported peak RSS belongs to one parse only:
``ru_maxrss`` is a process high-water mark that never falls, so measuring
several sizes inside one interpreter would report the largest run for all of
them.

    python -m tests.benchmark_ingestion <log_dir> [log_file]

Prints one JSON object describing the run. ``tests/test_eventlog.py`` drives it
across increasing fixtures; it is also usable by hand against a real log.
"""

from __future__ import annotations

import json
import resource
import sys
import time


def _peak_rss_bytes() -> int:
    """Peak resident set size of this process, in bytes.

    ``ru_maxrss`` is bytes on macOS and kilobytes on Linux.
    """
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak if sys.platform == "darwin" else peak * 1024


def measure(log_dir: str, log_file: str | None = None) -> dict:
    # Imported here so the interpreter/library startup cost is paid before the
    # baseline RSS reading below.
    from sparkparse.eventlog import resolve_source
    from sparkparse.parse import parse_source

    source = resolve_source(log_dir, log_file)
    baseline_rss = _peak_rss_bytes()

    start = time.perf_counter()
    parsed = parse_source(source)
    elapsed = time.perf_counter() - start

    peak_rss = _peak_rss_bytes()
    return {
        "log": source.root_uri,
        "events_read": parsed.events_read,
        "tasks": len(parsed.tasks),
        "stages": len(parsed.stages),
        "queries": len(parsed.queries),
        "elapsed_s": elapsed,
        "baseline_rss_bytes": baseline_rss,
        "peak_rss_bytes": peak_rss,
        "retained_rss_bytes": max(peak_rss - baseline_rss, 0),
    }


def main(argv: list[str]) -> int:
    if not argv:
        print(__doc__, file=sys.stderr)
        return 2
    log_dir = argv[0]
    log_file = argv[1] if len(argv) > 1 else None
    print(json.dumps(measure(log_dir, log_file)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
