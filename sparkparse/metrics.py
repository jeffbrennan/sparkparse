"""Canonical metric registry shared by the event-log and Spark Connect paths.

Spark operator metrics arrive under backend-specific names ("number of output
rows" on classic Spark, "numOutputRows" on Photon/Connect). This module maps the
verified aliases onto canonical names with an explicit unit, scope and
aggregation rule so downstream analysis compares like with like.

Deliberate omissions:

- Only aliases that have been observed in real plans are registered. An
  unrecognized metric is preserved verbatim with ``canonical=None`` and a unit
  inferred from its Spark metric type; it is never guessed onto a canonical name.
- ``numRowsScanned`` and ``numOutputRows`` are separate canonical metrics. Rows
  a scan read and rows it emitted are different quantities.
- Operator spill (``spill size`` / ``numBytesSpilled``) is a single canonical
  metric with an unspecified storage medium. It is not equated with, and never
  added to, the task-level memory/disk spill counters.
- ``data size`` describes an in-memory/broadcast payload, not bytes read from
  storage, so it is not an alias of ``scan_bytes``.
- Photon's ``numBytesRead``/``numBytesWritten`` mean shuffle bytes on an
  exchange and file bytes on a scan. The name alone does not say which, so they
  stay unmapped.

Integer precision: counts survive intact end to end. ``accumulator_totals.value``
is a Polars ``Float64`` — one column also has to hold fractional timings — so
both ingestion paths carry an ``value_exact`` ``Int64`` field alongside it,
populated for every metric that was not rescaled. Normalization reads
``value_exact`` when it is present and never routes an integer through ``float``,
so a count above 2**53 is preserved from the event log or Connect server all the
way to the JSON export.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from sparkparse.models import (
    CapabilityStatus,
    MetricAggregation,
    MetricDefinition,
    MetricDerivation,
    MetricScope,
    MetricUnit,
    NormalizedMetric,
)

# Source labels attached to every normalized metric.
SOURCE_EVENT_LOG = "event_log_accumulator"
SOURCE_CONNECT_PLAN = "connect_plan_metric"
SOURCE_TASK_METRICS = "task_metrics"


def _definition(
    canonical: str,
    unit: MetricUnit,
    aggregation: MetricAggregation,
    aliases: Iterable[str],
    description: str,
    scope: MetricScope = MetricScope.operator,
) -> MetricDefinition:
    return MetricDefinition(
        canonical=canonical,
        unit=unit,
        scope=scope,
        aggregation=aggregation,
        aliases=frozenset(aliases),
        description=description,
    )


METRIC_REGISTRY: tuple[MetricDefinition, ...] = (
    _definition(
        "output_rows",
        MetricUnit.rows,
        MetricAggregation.sum,
        ["number of output rows", "numOutputRows"],
        "Rows emitted by the operator, summed over tasks.",
    ),
    _definition(
        "scanned_rows",
        MetricUnit.rows,
        MetricAggregation.sum,
        ["numRowsScanned"],
        "Rows read by a scan before pushed filters removed any. Not output rows.",
    ),
    _definition(
        "files_read",
        MetricUnit.items,
        MetricAggregation.sum,
        ["number of files read", "numFiles"],
        "Files opened by a scan.",
    ),
    _definition(
        "scan_bytes",
        MetricUnit.bytes,
        MetricAggregation.sum,
        ["size of files read"],
        "Bytes of source files a scan read. Classic Spark only; no verified "
        "Photon alias, so Photon scans report no canonical scan bytes.",
    ),
    _definition(
        "scan_time",
        MetricUnit.milliseconds,
        MetricAggregation.sum,
        ["scan time"],
        "Time spent reading source data, summed over tasks.",
    ),
    _definition(
        "metadata_time",
        MetricUnit.milliseconds,
        MetricAggregation.sum,
        ["metadata time"],
        "Time spent listing files and resolving partitions.",
    ),
    _definition(
        "operator_time",
        MetricUnit.milliseconds,
        MetricAggregation.sum,
        ["duration", "exclusiveTime"],
        "Time spent inside this operator itself, excluding its children, summed "
        "over tasks.",
    ),
    _definition(
        "cumulative_operator_time",
        MetricUnit.milliseconds,
        MetricAggregation.cumulative,
        ["cumulTime"],
        "Operator time summed over the whole subtree and all tasks. Not wall "
        "clock; summing it across nodes double counts children.",
    ),
    _definition(
        "spill_bytes",
        MetricUnit.bytes,
        MetricAggregation.sum,
        ["spill size", "numBytesSpilled"],
        "Bytes this operator spilled. Storage medium is unspecified, so it is "
        "neither memory nor disk spill in the task-metric sense.",
    ),
    _definition(
        "peak_memory_bytes",
        MetricUnit.bytes,
        MetricAggregation.max,
        ["peak memory", "peakMemoryUsage", "peakMemUsage"],
        "Peak execution memory held by the operator in one task.",
    ),
    _definition(
        "shuffle_write_bytes",
        MetricUnit.bytes,
        MetricAggregation.sum,
        ["shuffle bytes written"],
        "Bytes written to shuffle by this operator.",
    ),
    _definition(
        "shuffle_write_rows",
        MetricUnit.rows,
        MetricAggregation.sum,
        ["shuffle records written"],
        "Records written to shuffle by this operator.",
    ),
    _definition(
        "shuffle_write_time",
        MetricUnit.milliseconds,
        MetricAggregation.sum,
        ["shuffle write time"],
        "Time spent writing shuffle data.",
    ),
    _definition(
        "shuffle_remote_read_bytes",
        MetricUnit.bytes,
        MetricAggregation.sum,
        ["remote bytes read", "remoteBytesRead"],
        "Shuffle bytes fetched from other executors.",
    ),
    _definition(
        "shuffle_local_read_bytes",
        MetricUnit.bytes,
        MetricAggregation.sum,
        ["local bytes read", "localBytesRead"],
        "Shuffle bytes read from the local executor.",
    ),
    _definition(
        "shuffle_read_rows",
        MetricUnit.rows,
        MetricAggregation.sum,
        ["records read"],
        "Records read from shuffle.",
    ),
    _definition(
        "shuffle_fetch_wait_time",
        MetricUnit.milliseconds,
        MetricAggregation.sum,
        ["fetch wait time", "fetchWaitTime"],
        "Time blocked waiting for remote shuffle blocks.",
    ),
    _definition(
        "broadcast_payload_bytes",
        MetricUnit.bytes,
        MetricAggregation.sum,
        ["data size"],
        "In-memory size of a broadcast or exchange payload. Not bytes read "
        "from storage.",
    ),
    _definition(
        "partitions",
        MetricUnit.items,
        MetricAggregation.sum,
        ["number of partitions"],
        "Partitions produced by the operator.",
    ),
    _definition(
        "skewed_partitions",
        MetricUnit.items,
        MetricAggregation.sum,
        ["number of skewed partitions", "numSkewedPartitions"],
        "Partitions adaptive query execution identified as skewed and split.",
    ),
    _definition(
        "skewed_partition_splits",
        MetricUnit.items,
        MetricAggregation.sum,
        ["number of skewed partition splits"],
        "Splits adaptive query execution created from skewed partitions.",
    ),
    _definition(
        "partition_data_size",
        MetricUnit.bytes,
        MetricAggregation.sum,
        ["partition data size"],
        "Size of the shuffle partitions an AQE shuffle read consumed.",
    ),
)

_ALIAS_INDEX: dict[str, MetricDefinition] = {
    alias: definition for definition in METRIC_REGISTRY for alias in definition.aliases
}

CANONICAL_INDEX: dict[str, MetricDefinition] = {
    definition.canonical: definition for definition in METRIC_REGISTRY
}

# Spark metric types, used only when a raw name has no registered definition.
_TYPE_UNITS: dict[str, MetricUnit] = {
    "size": MetricUnit.bytes,
    "timing": MetricUnit.milliseconds,
    "nsTiming": MetricUnit.nanoseconds,
    "sum": MetricUnit.items,
    "average": MetricUnit.items,
}

_INTEGRAL_UNITS = frozenset({MetricUnit.rows, MetricUnit.items})


def definition_for(raw_name: str) -> MetricDefinition | None:
    """Return the registered definition for a raw metric name, if any."""
    return _ALIAS_INDEX.get(raw_name)


def _coerce_value(value: Any, unit: MetricUnit) -> float | int | None:
    """Return a numeric value, keeping counts exact and preserving a real zero.

    An integer input is never routed through ``float``: above 2**53 that would
    silently round the count. Integral strings are parsed as integers for the
    same reason. Only values that are genuinely floating point stay floats, and
    a float carrying an integral count is narrowed back to ``int``.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        text = value.strip()
        try:
            return int(text)
        except ValueError:
            pass
        try:
            numeric = float(text)
        except ValueError:
            return None
    elif isinstance(value, float):
        numeric = value
    else:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return None
    if unit in _INTEGRAL_UNITS and numeric.is_integer():
        return int(numeric)
    return numeric


def normalize_metric(
    raw: Any, source: str = SOURCE_EVENT_LOG
) -> NormalizedMetric | None:
    """Normalize one accumulator-total struct into a :class:`NormalizedMetric`."""
    if not isinstance(raw, dict):
        return None
    raw_name = raw.get("metric_name")
    if not raw_name:
        return None

    definition = definition_for(str(raw_name))
    if definition is not None:
        unit = definition.unit
        derivation = MetricDerivation.measured
        canonical: str | None = definition.canonical
        scope = definition.scope
        aggregation = definition.aggregation
    else:
        canonical = None
        unit = _TYPE_UNITS.get(str(raw.get("metric_type") or ""), MetricUnit.none)
        derivation = MetricDerivation.inferred_from_metric_type
        scope = MetricScope.operator
        aggregation = MetricAggregation.unknown

    # ``value_exact`` carries the unrounded integer when the frame's float64
    # ``value`` column could not hold the count exactly.
    exact = raw.get("value_exact")
    value = _coerce_value(
        exact
        if isinstance(exact, int) and not isinstance(exact, bool)
        else raw.get("value"),
        unit,
    )
    return NormalizedMetric(
        raw_name=str(raw_name),
        canonical=canonical,
        value=value,
        unit=unit,
        scope=scope,
        aggregation=aggregation,
        source=source,
        derivation=derivation,
        coverage=(
            CapabilityStatus.available
            if value is not None
            else CapabilityStatus.unavailable
        ),
        readable=raw.get("readable_str"),
    )


def normalize_metrics(
    acc_totals: Iterable[dict[str, Any]] | None, source: str = SOURCE_EVENT_LOG
) -> list[NormalizedMetric]:
    """Normalize a node's accumulator totals, preserving unknown metrics."""
    if not acc_totals:
        return []
    normalized: list[NormalizedMetric] = []
    for raw in acc_totals:
        metric = normalize_metric(raw, source)
        if metric is not None:
            normalized.append(metric)
    return normalized


def canonical_metrics(
    acc_totals: Iterable[dict[str, Any]] | None, source: str = SOURCE_EVENT_LOG
) -> dict[str, NormalizedMetric]:
    """Return canonical name -> metric for a node's accumulator totals.

    When one canonical name has several raw aliases present, the first metric
    carrying a value wins; a later alias never overwrites a measured value.
    """
    resolved: dict[str, NormalizedMetric] = {}
    for metric in normalize_metrics(acc_totals, source):
        if metric.canonical is None:
            continue
        existing = resolved.get(metric.canonical)
        if existing is not None and existing.value is not None:
            continue
        resolved[metric.canonical] = metric
    return resolved


def metric_value(
    acc_totals: Iterable[dict[str, Any]] | None,
    canonical: str,
    source: str = SOURCE_EVENT_LOG,
) -> float | int | None:
    """Return the value for a canonical metric, or ``None`` when absent.

    A measured zero is returned as ``0``; only a genuinely missing metric is
    ``None``.
    """
    metric = canonical_metrics(acc_totals, source).get(canonical)
    if metric is None:
        return None
    return metric.value
