"""Discovery and streaming reads of Spark event-log sources.

A *logical* event log is one application attempt. On disk it is either a single
file (``<appId>[_<attemptId>][.<codec>][.inprogress]``) or, when
``spark.eventLog.rolling.enabled`` is set, a directory
``eventlog_v2_<appId>[_<attemptId>]/`` holding an ``appstatus_*`` marker and
numbered ``events_<index>_<appId>[.<codec>][.inprogress]`` segments.

Both shapes are read the same way: as one ordered stream of lines, never as a
whole-file string.

Supported codecs
----------------
``none``, ``zstd`` (needs the ``zstandard`` package) and ``gz`` (stdlib).
Spark's ``lz4``, ``lzf`` and ``snappy`` event logs use Java-specific block
framing that no Python codec reads; those raise an explicit error naming the
codec rather than failing as corrupt JSON.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterator
from pathlib import Path
from typing import IO, Any

from sparkparse.models import (
    READABLE_CODECS,
    EventLogCodec,
    EventLogSegment,
    EventLogSource,
)
from sparkparse.storage import (
    get_path_name,
    is_cloud_path,
    join_path,
    list_files,
    open_file,
)

logger = logging.getLogger(__name__)

ROLLING_DIR_PREFIX = "eventlog_v2_"
ROLLING_STATUS_PREFIX = "appstatus_"
ROLLING_EVENTS_PATTERN = re.compile(r"^events_(\d+)_(.+)$")
IN_PROGRESS_SUFFIX = ".inprogress"

# Files a Spark log directory accumulates that are not event logs.
IGNORED_NAMES = {".DS_Store", "_SUCCESS", "_started", "_committed"}
IGNORED_SUFFIXES = (".crc", ".tmp", ".lock")


class UnsupportedCodecError(RuntimeError):
    """Raised for an event log written with a codec Python cannot read."""


class EventLogNotFoundError(FileNotFoundError):
    """Raised when a log directory holds no readable event-log source."""


def _is_ignored(name: str) -> bool:
    if name in IGNORED_NAMES or name.startswith("."):
        return True
    return name.endswith(IGNORED_SUFFIXES)


def _split_codec(stem: str) -> tuple[str, EventLogCodec]:
    """Split a trailing ``.<codec>`` off an event-log file name."""
    if "." not in stem:
        return stem, EventLogCodec.none
    base, _, ext = stem.rpartition(".")
    normalized = {"gzip": "gz"}.get(ext.lower(), ext.lower())
    try:
        codec = EventLogCodec(normalized)
    except ValueError:
        return stem, EventLogCodec.none
    if codec is EventLogCodec.none:
        return base, EventLogCodec.none
    return base, codec


def _parse_file_name(name: str) -> tuple[str, EventLogCodec, bool]:
    """Return (identity, codec, in_progress) for an event-log file name."""
    in_progress = name.endswith(IN_PROGRESS_SUFFIX)
    stem = name.removesuffix(IN_PROGRESS_SUFFIX) if in_progress else name
    identity, codec = _split_codec(stem)
    return identity, codec, in_progress


def _modified_time(uri: str) -> float | None:
    if is_cloud_path(uri):
        return None
    try:
        return Path(uri).stat().st_mtime
    except OSError:
        return None


def _rolling_source(dir_uri: str) -> EventLogSource | None:
    """Build a source from an ``eventlog_v2_*`` rolling-log directory."""
    dir_name = get_path_name(dir_uri)
    app_identity = dir_name.removeprefix(ROLLING_DIR_PREFIX)

    segments: list[EventLogSegment] = []
    status_in_progress = False
    saw_status = False
    for entry in list_files(dir_uri):
        entry_name = get_path_name(entry)
        if _is_ignored(entry_name):
            continue
        if entry_name.startswith(ROLLING_STATUS_PREFIX):
            saw_status = True
            status_in_progress = entry_name.endswith(IN_PROGRESS_SUFFIX)
            continue
        identity, codec, in_progress = _parse_file_name(entry_name)
        match = ROLLING_EVENTS_PATTERN.match(identity)
        if not match:
            logger.debug("Ignoring non-segment file in rolling log dir: %s", entry)
            continue
        segments.append(
            EventLogSegment(
                uri=entry,
                index=int(match.group(1)),
                codec=codec,
                in_progress=in_progress,
            )
        )

    if not segments:
        return None

    segments.sort(key=lambda segment: (segment.index or 0, segment.uri))
    modified = [
        mtime
        for mtime in (_modified_time(s.uri) for s in segments)
        if mtime is not None
    ]
    return EventLogSource(
        name=dir_name,
        application_id=app_identity,
        segments=segments,
        rolling=True,
        # An in-progress status marker, or no marker at all, means the
        # application never wrote its completion record.
        complete=saw_status and not status_in_progress,
        modified=max(modified) if modified else None,
        root_uri=dir_uri,
    )


def single_file_source(uri: str) -> EventLogSource:
    name = get_path_name(uri)
    identity, codec, in_progress = _parse_file_name(name)
    return EventLogSource(
        name=identity,
        application_id=identity,
        segments=[EventLogSegment(uri=uri, codec=codec, in_progress=in_progress)],
        rolling=False,
        complete=not in_progress,
        modified=_modified_time(uri),
        root_uri=uri,
    )


def _is_dir(uri: str) -> bool:
    if is_cloud_path(uri):
        # Cloud object stores have no directories; a rolling log shows up as a
        # prefix, which list_files enumerates.
        return bool(get_path_name(uri).startswith(ROLLING_DIR_PREFIX))
    return Path(uri).is_dir()


def discover_sources(log_dir: str | Path) -> list[EventLogSource]:
    """Return every logical event log under ``log_dir``.

    Marker files, checksums and hidden files are ignored. Rolling-log
    directories collapse into one source with ordered segments.
    """
    log_dir_str = str(log_dir)
    sources: list[EventLogSource] = []
    for entry in list_files(log_dir_str):
        entry_name = get_path_name(entry)
        if _is_ignored(entry_name):
            continue
        if entry_name.startswith(ROLLING_DIR_PREFIX) or _is_dir(entry):
            if not entry_name.startswith(ROLLING_DIR_PREFIX):
                logger.debug("Ignoring non-event-log directory: %s", entry)
                continue
            source = _rolling_source(entry)
            if source is not None:
                sources.append(source)
            continue
        sources.append(single_file_source(entry))
    return sources


def _sort_key(source: EventLogSource) -> tuple[float, str]:
    # Newest policy: modification time when the filesystem reports one,
    # otherwise the name. Ties fall back to the name so the order is stable.
    return (
        source.modified if source.modified is not None else float("-inf"),
        source.name,
    )


def newest_source(sources: list[EventLogSource]) -> EventLogSource:
    """Return the most recently modified source (name order breaks ties)."""
    return max(sources, key=_sort_key)


def resolve_source(log_dir: str | Path, log_file: str | None = None) -> EventLogSource:
    """Resolve one event-log source from ``log_dir``.

    ``log_file`` selects a source explicitly, by file name, rolling-directory
    name, application id, or full path. Without it the newest source is used:
    most recent modification time, falling back to name order when the
    filesystem reports no mtime (cloud object listings).
    """
    log_dir_str = str(log_dir)
    if log_file is not None:
        candidate = log_file if is_cloud_path(log_file) else str(log_file)
        direct = candidate if "/" in candidate else join_path(log_dir_str, candidate)
        if _is_dir(direct) or get_path_name(direct).startswith(ROLLING_DIR_PREFIX):
            source = _rolling_source(direct)
            if source is None:
                raise EventLogNotFoundError(
                    f"No event-log segments found in rolling log dir: {direct}"
                )
            return source
        matches = [
            source
            for source in discover_sources(log_dir_str)
            if log_file in (source.name, get_path_name(source.root_uri or ""))
            or source.application_id == log_file
            or source.root_uri == log_file
        ]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            raise ValueError(
                f"'{log_file}' matches {len(matches)} event logs in {log_dir_str}: "
                f"{[m.root_uri for m in matches]}"
            )
        return single_file_source(direct)

    sources = discover_sources(log_dir_str)
    if not sources:
        raise EventLogNotFoundError(f"No event log files found in: {log_dir_str}")
    return newest_source(sources)


def _open_segment(segment: EventLogSegment) -> IO[Any]:
    """Open one segment as a text stream, decompressing as needed."""
    if segment.codec not in READABLE_CODECS:
        raise UnsupportedCodecError(
            f"Event log {segment.uri} is compressed with '{segment.codec}', which "
            "uses Java-specific framing that Python cannot decode. Re-run with "
            "spark.eventLog.compression.codec=zstd (or none), or decompress the "
            "log with Spark's history server tooling first. "
            f"Readable codecs: {sorted(c.value for c in READABLE_CODECS)}."
        )

    if segment.codec is EventLogCodec.none:
        return open_file(segment.uri, "r")

    raw = open_file(segment.uri, "rb")
    if segment.codec is EventLogCodec.gz:
        import gzip
        import io

        return io.TextIOWrapper(gzip.GzipFile(fileobj=raw), encoding="utf-8")

    try:
        import zstandard
    except ImportError as exc:  # pragma: no cover - exercised only without zstandard
        raise UnsupportedCodecError(
            f"Reading {segment.uri} needs the 'zstandard' package: "
            "install it with `uv add zstandard` or the sparkparse[zstd] extra."
        ) from exc
    import io

    reader = zstandard.ZstdDecompressor().stream_reader(raw)
    return io.TextIOWrapper(reader, encoding="utf-8")


def iter_lines(source: EventLogSource) -> Iterator[tuple[str, int, str]]:
    """Yield ``(uri, line_number, line)`` across the source's segments.

    Segments are read in order and one line at a time, so peak memory does not
    scale with log size.
    """
    for segment in source.segments:
        with _open_segment(segment) as handle:
            for line_number, line in enumerate(handle, start=1):
                yield segment.uri, line_number, line
