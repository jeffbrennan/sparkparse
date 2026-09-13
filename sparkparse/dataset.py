"""Server-side, cached access to whatever a dashboard was pointed at.

A dashboard source is one of:

- a directory of raw Spark event logs
- a saved capture artifact (directory or ``.json`` file)
- an in-memory ``CaptureResult`` or ``ParsedLogDataFrames``

The dataset parses each log at most once and keeps the frames on the server, so
Dash callbacks do not re-read event logs or re-ship task tables on every
interaction.
"""

from __future__ import annotations

import datetime
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, cast

from sparkparse.artifact import is_capture_artifact, load_capture_artifact
from sparkparse.eventlog import discover_sources
from sparkparse.models import (
    CaptureCapabilities,
    CaptureMetadata,
    CaptureResult,
    EventLogSource,
    ParsedLogDataFrames,
)
from sparkparse.parse import get_parsed_metrics, source_has_queries

logger = logging.getLogger(__name__)

# Cap on task rows shipped to the browser for one drilldown. Summary metrics are
# computed from the full cached frame, so only the interactive table is bounded.
MAX_BROWSER_TASK_ROWS = 50_000


class Dataset:
    """Cached frames for a raw-log directory, artifact, or in-memory result."""

    def __init__(self, source: str | Path | CaptureResult | ParsedLogDataFrames):
        self._lock = threading.Lock()
        self._cache: dict[str, ParsedLogDataFrames] = {}
        self._results: dict[str, CaptureResult] = {}
        self._memory_result: CaptureResult | None = None
        self._artifact_path: str | None = None
        self._log_dir: str | None = None
        self._order: list[str] = []

        if isinstance(source, CaptureResult):
            self.kind = "memory"
            self._memory_result = source
            self._label = source.metadata.workload_label or "capture"
        elif isinstance(source, ParsedLogDataFrames):
            self.kind = "memory"
            self._memory_result = CaptureResult(
                dag=source.dag,
                combined=source.combined,
                metadata=CaptureMetadata(
                    capture_id="memory",
                    backend="memory",
                    capture_start=_utcnow(),
                    workload_label="capture",
                ),
                capabilities=CaptureCapabilities(),
            )
            self._label = "capture"
        elif is_capture_artifact(source):
            self.kind = "artifact"
            self._memory_result = load_capture_artifact(source)
            self._label = self._memory_result.metadata.workload_label or _stem(source)
        else:
            self.kind = "event_log"
            self._log_dir = str(source)
            self._label = _stem(source)

    @property
    def label(self) -> str:
        return self._label

    def list_logs(self) -> list[str]:
        if self.kind != "event_log":
            assert self._memory_result is not None
            return [self._label]
        assert self._log_dir is not None
        sources = discover_sources(self._log_dir)
        # Checking for queries streams each log; overlap the I/O.
        with ThreadPoolExecutor(max_workers=8) as executor:
            has_queries = list(executor.map(source_has_queries, sources))
        return sorted(
            source.name for source, has in zip(sources, has_queries, strict=True) if has
        )

    def event_log_source(self, log_name: str) -> EventLogSource | None:
        if self.kind != "event_log":
            return None
        assert self._log_dir is not None
        for source in discover_sources(self._log_dir):
            if source.name == log_name:
                return source
        return None

    def dataframes(self, log_name: str | None) -> ParsedLogDataFrames:
        if self.kind != "event_log":
            assert self._memory_result is not None
            return ParsedLogDataFrames(
                dag=self._memory_result.dag,
                combined=self._memory_result.combined,
            )

        name = log_name or self.label
        with self._lock:
            cached = self._cache.get(name)
            if cached is not None:
                return cached
        assert self._log_dir is not None
        dfs = get_parsed_metrics(
            log_dir=self._log_dir, log_file=name, out_dir=None, out_format=None
        )
        with self._lock:
            self._cache[name] = dfs
        return dfs

    def result(self, log_name: str | None) -> CaptureResult | None:
        if self.kind != "event_log":
            return self._memory_result
        return self._results.get(log_name or self.label)

    def capabilities(self, log_name: str | None) -> CaptureCapabilities | None:
        result = self.result(log_name)
        return result.capabilities if result is not None else None

    def metadata(self, log_name: str | None) -> CaptureMetadata | None:
        result = self.result(log_name)
        return result.metadata if result is not None else None

    def combined_records(
        self, log_name: str | None, limit: int = MAX_BROWSER_TASK_ROWS
    ) -> tuple[list[dict[str, Any]], bool]:
        """Return task records capped at ``limit`` and whether truncation happened."""
        combined = self.dataframes(log_name).combined
        truncated = combined.height > limit
        frame = combined.head(limit) if truncated else combined
        records = cast(list[dict[str, Any]], frame.to_pandas().to_dict("records"))
        return records, truncated


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC)


def _stem(path: str | Path) -> str:
    name = str(path).rstrip("/").rsplit("/", 1)[-1]
    if "." in name:
        return name.rsplit(".", 1)[0]
    return name
