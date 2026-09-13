"""Increment 3 of brief 04: source discovery, rolled segments, streaming.

The streaming contract is checked directly — the reader must never call
``read()``/``readlines()`` on a log file — because a memory assertion alone
cannot distinguish a whole-file copy from ordinary retained state.
"""

from __future__ import annotations

import builtins
import gzip
import importlib.util
import json
import subprocess
import sys
import time
from pathlib import Path

import pytest

from sparkparse.eventlog import (
    EventLogNotFoundError,
    UnsupportedCodecError,
    discover_sources,
    iter_lines,
    resolve_source,
)
from sparkparse.models import EventLogCodec
from sparkparse.parse import get_parsed_metrics, parse_source
from tests.eventlog_fixtures import (
    log_lines,
    rolling_log_dir,
    scale_task_volume,
    write_log,
)

HAS_ZSTD = importlib.util.find_spec("zstandard") is not None


def _dag_records(dfs) -> list[dict]:
    return dfs.dag.sort("query_id", "node_id").to_dicts()


# ---------------------------------------------------------------------------
# discovery
# ---------------------------------------------------------------------------


def test_marker_files_and_checksums_are_not_event_logs(tmp_path):
    write_log(tmp_path / "app-0001", log_lines())
    (tmp_path / ".DS_Store").write_text("junk")
    (tmp_path / "_SUCCESS").write_text("")
    (tmp_path / ".app-0001.crc").write_text("")
    (tmp_path / "notes.tmp").write_text("")

    sources = discover_sources(tmp_path)

    assert [source.name for source in sources] == ["app-0001"]


def test_rolling_log_directory_is_one_source_with_ordered_segments(tmp_path):
    rolling_log_dir(tmp_path, log_lines(), app_id="app-rolled", segments=3)

    sources = discover_sources(tmp_path)

    assert len(sources) == 1
    source = sources[0]
    assert source.rolling is True
    assert source.complete is True
    assert [segment.index for segment in source.segments] == [1, 2, 3]
    assert source.application_id == "app-rolled"


def test_in_progress_logs_are_marked_incomplete(tmp_path):
    write_log(tmp_path / "app-live.inprogress", log_lines())
    rolling_log_dir(tmp_path, log_lines(), app_id="app-live-rolled", in_progress=True)

    sources = {source.name: source for source in discover_sources(tmp_path)}

    assert sources["app-live"].complete is False
    assert sources["app-live"].segments[0].in_progress is True
    assert sources["eventlog_v2_app-live-rolled"].complete is False


def test_codec_suffix_is_recognized_without_being_read(tmp_path):
    (tmp_path / "app-0001.lz4").write_bytes(b"\x04\x22\x4d\x18binary")

    source = discover_sources(tmp_path)[0]

    assert source.application_id == "app-0001"
    assert source.segments[0].codec is EventLogCodec.lz4


def test_newest_source_wins_by_modification_time_not_name(tmp_path):
    write_log(tmp_path / "zzz-old", log_lines())
    time.sleep(0.01)
    write_log(tmp_path / "aaa-new", log_lines())

    assert resolve_source(tmp_path).name == "aaa-new"


def test_explicit_selection_accepts_name_app_id_and_path(tmp_path):
    write_log(tmp_path / "app-a", log_lines())
    write_log(tmp_path / "app-b", log_lines())

    for selector in ("app-a", str(tmp_path / "app-a")):
        assert resolve_source(tmp_path, selector).name == "app-a"


def test_explicit_selection_of_a_rolling_directory(tmp_path):
    rolling_log_dir(tmp_path, log_lines(), app_id="app-rolled")

    source = resolve_source(tmp_path, "eventlog_v2_app-rolled")

    assert source.rolling is True
    assert len(source.segments) == 2


def test_empty_directory_raises_a_typed_error(tmp_path):
    with pytest.raises(EventLogNotFoundError):
        resolve_source(tmp_path)


# ---------------------------------------------------------------------------
# reading
# ---------------------------------------------------------------------------


def test_rolled_segments_parse_identically_to_one_file(tmp_path):
    lines = log_lines()
    write_log(tmp_path / "single" / "app-single", lines)
    rolling_log_dir(tmp_path / "rolled", lines, app_id="app-single", segments=4)

    single = get_parsed_metrics(
        log_dir=tmp_path / "single", out_dir=None, out_format=None
    )
    rolled = get_parsed_metrics(
        log_dir=tmp_path / "rolled", out_dir=None, out_format=None
    )

    assert _dag_records(rolled) == _dag_records(single)
    assert rolled.combined.drop("log_name", "parsed_log_name").equals(
        single.combined.drop("log_name", "parsed_log_name")
    )


def test_segment_boundaries_do_not_split_events(tmp_path):
    """Each segment holds whole lines; the reader must not glue them wrongly."""
    lines = log_lines()
    rolling_log_dir(tmp_path, lines, app_id="app-rolled", segments=5)

    source = resolve_source(tmp_path)
    read_back = [line.rstrip("\n") for _, _, line in iter_lines(source)]

    assert read_back == lines
    assert all(json.loads(line) for line in read_back)


def test_gzip_compressed_log_reads_like_an_uncompressed_one(tmp_path):
    lines = log_lines()
    write_log(tmp_path / "plain" / "app-0001", lines)
    (tmp_path / "gz").mkdir()
    with gzip.open(tmp_path / "gz" / "app-0001.gz", "wt") as handle:
        handle.write("\n".join(lines) + "\n")

    plain = get_parsed_metrics(
        log_dir=tmp_path / "plain", out_dir=None, out_format=None
    )
    compressed = get_parsed_metrics(
        log_dir=tmp_path / "gz", out_dir=None, out_format=None
    )

    assert _dag_records(compressed) == _dag_records(plain)


@pytest.mark.skipif(not HAS_ZSTD, reason="zstandard is not installed")
def test_zstd_compressed_log_reads_like_an_uncompressed_one(tmp_path):
    import zstandard

    lines = log_lines()
    write_log(tmp_path / "plain" / "app-0001", lines)
    (tmp_path / "zstd").mkdir()
    payload = ("\n".join(lines) + "\n").encode()
    (tmp_path / "zstd" / "app-0001.zstd").write_bytes(
        zstandard.ZstdCompressor().compress(payload)
    )

    plain = get_parsed_metrics(
        log_dir=tmp_path / "plain", out_dir=None, out_format=None
    )
    compressed = get_parsed_metrics(
        log_dir=tmp_path / "zstd", out_dir=None, out_format=None
    )

    assert _dag_records(compressed) == _dag_records(plain)


@pytest.mark.parametrize("codec", ["lz4", "snappy", "lzf"])
def test_java_framed_codecs_fail_by_name_not_as_corrupt_json(tmp_path, codec):
    (tmp_path / f"app-0001.{codec}").write_bytes(b"not really compressed")

    with pytest.raises(UnsupportedCodecError, match=codec):
        get_parsed_metrics(log_dir=tmp_path, out_dir=None, out_format=None)


def test_reading_never_copies_the_whole_file(tmp_path, monkeypatch):
    """The parser must stream: no read()/readlines() on the log handle."""
    path = write_log(tmp_path / "app-0001", log_lines())
    real_open = builtins.open
    whole_file_reads: list[str] = []

    class _WatchedFile:
        def __init__(self, handle, name):
            self._handle = handle
            self._name = name

        def read(self, *args, **kwargs):
            whole_file_reads.append(f"read:{self._name}")
            return self._handle.read(*args, **kwargs)

        def readlines(self, *args, **kwargs):
            whole_file_reads.append(f"readlines:{self._name}")
            return self._handle.readlines(*args, **kwargs)

        def __iter__(self):
            return iter(self._handle)

        def __enter__(self):
            self._handle.__enter__()
            return self

        def __exit__(self, *args):
            return self._handle.__exit__(*args)

        def __getattr__(self, item):
            return getattr(self._handle, item)

    def watched_open(file, *args, **kwargs):
        handle = real_open(file, *args, **kwargs)
        if str(file) == str(path):
            return _WatchedFile(handle, str(file))
        return handle

    monkeypatch.setattr(builtins, "open", watched_open)
    parsed = parse_source(resolve_source(tmp_path))

    assert parsed.queries
    assert whole_file_reads == []


# Measured on the recorded fixtures: ~36 KB of retained model state per task.
# The bound is generous headroom over that, tight enough to catch a regression
# that starts holding raw event dicts or file text.
_MAX_RETAINED_BYTES_PER_TASK = 200_000


def _benchmark(log_dir: Path, log_file: str) -> dict:
    """Run one ingestion measurement in a fresh interpreter."""
    completed = subprocess.run(
        [sys.executable, "-m", "tests.benchmark_ingestion", str(log_dir), log_file],
        capture_output=True,
        text=True,
        check=True,
        cwd=Path(__file__).parents[1],
    )
    return json.loads(completed.stdout.strip().splitlines()[-1])


def test_ingestion_cost_scales_with_retained_state_not_file_size(tmp_path):
    """Benchmark: elapsed time and process peak RSS on increasing fixtures.

    Each size is parsed end to end (``parse_source``, models and all) in its own
    process, because ``ru_maxrss`` is a high-water mark that would otherwise
    report the largest run for every size.

    Peak RSS grows because every task, stage and plan node is retained as a
    model until the frames are built — that growth is expected and is what the
    per-task bound below documents. What must not grow is a copy of the log
    text, which ``test_reading_never_copies_the_whole_file`` pins down directly.
    """
    base = log_lines()
    measurements = []
    for multiplier in (1, 5, 20):
        name = f"app-{multiplier}x"
        path = write_log(tmp_path / name, scale_task_volume(base, multiplier - 1))
        result = _benchmark(tmp_path, name)
        result["size_bytes"] = path.stat().st_size
        measurements.append(result)

    for result in measurements:
        print(
            f"{Path(result['log']).name}: bytes={result['size_bytes']} "
            f"events={result['events_read']} tasks={result['tasks']} "
            f"elapsed={result['elapsed_s']:.4f}s "
            f"peak_rss={result['peak_rss_bytes']} "
            f"retained_rss={result['retained_rss_bytes']}"
        )

    small, large = measurements[0], measurements[-1]

    # The larger fixtures are valid logs, not padding: same plan, more tasks.
    assert {result["queries"] for result in measurements} == {small["queries"]}
    assert large["tasks"] == small["tasks"] * 20

    # Retained state is proportional to the tasks kept, not to the bytes read.
    assert large["retained_rss_bytes"] < _MAX_RETAINED_BYTES_PER_TASK * large["tasks"]

    # Sub-quadratic: 20x the tasks must not cost anywhere near 400x the time.
    task_ratio = large["tasks"] / small["tasks"]
    assert large["elapsed_s"] < small["elapsed_s"] * task_ratio * 3


def test_cloud_style_source_is_discovered_and_streamed(monkeypatch):
    """A fake object store exercises the fsspec path without real credentials."""
    fsspec = pytest.importorskip("fsspec")
    import sparkparse.storage as storage

    memory_fs = fsspec.filesystem("memory")
    for existing in memory_fs.ls("/", detail=False):
        memory_fs.rm(existing, recursive=True)

    monkeypatch.setattr(
        storage, "CLOUD_PREFIXES", (*storage.CLOUD_PREFIXES, "memory://")
    )

    lines = log_lines()
    with fsspec.open("memory://logs/app-cloud", "w") as handle:
        handle.write("\n".join(lines) + "\n")
    with fsspec.open("memory://logs/_SUCCESS", "w") as handle:
        handle.write("")

    sources = discover_sources("memory://logs")
    assert [source.name for source in sources] == ["app-cloud"]

    cloud = get_parsed_metrics(log_dir="memory://logs", out_dir=None, out_format=None)
    local = get_parsed_metrics(
        log_dir=Path("tests/data/full_logs"),
        log_file="nested_loop_join",
        out_dir=None,
        out_format=None,
    )

    assert _dag_records(cloud) == _dag_records(local)


def test_dashboard_log_duration_streams_a_rolling_source(tmp_path):
    """The home page lists logical sources, including rolled ones."""
    from sparkparse.pages.home import get_log_duration

    rolling_log_dir(tmp_path, log_lines(), app_id="app-rolled", segments=3)
    source = resolve_source(tmp_path)

    duration = get_log_duration(source)

    assert duration.start_time <= duration.end_time
    assert duration.duration_seconds >= 0
    assert duration.duration_formatted
