import json

from sparkparse.metrics import (
    METRIC_REGISTRY,
    SOURCE_CONNECT_PLAN,
    canonical_metrics,
    definition_for,
    metric_value,
    normalize_metric,
    normalize_metrics,
)
from sparkparse.models import (
    CapabilityStatus,
    MetricAggregation,
    MetricDerivation,
    MetricUnit,
)
from tests.synthetic import accum

# ---------------------------------------------------------------------------
# registry
# ---------------------------------------------------------------------------


def test_registry_aliases_are_unique():
    seen: set[str] = set()
    for definition in METRIC_REGISTRY:
        for alias in definition.aliases:
            assert alias not in seen, f"{alias} is registered twice"
            seen.add(alias)


def test_canonical_names_are_unique():
    names = [definition.canonical for definition in METRIC_REGISTRY]
    assert len(names) == len(set(names))


def test_classic_and_photon_output_rows_agree():
    classic = canonical_metrics([accum("number of output rows", 500)])
    photon = canonical_metrics([accum("numOutputRows", 500)], SOURCE_CONNECT_PLAN)
    assert classic["output_rows"].value == photon["output_rows"].value == 500
    assert classic["output_rows"].unit == photon["output_rows"].unit
    assert classic["output_rows"].aggregation == photon["output_rows"].aggregation


def test_scanned_rows_is_not_output_rows():
    metrics = canonical_metrics(
        [accum("numRowsScanned", 1_000_000), accum("numOutputRows", 10)],
        SOURCE_CONNECT_PLAN,
    )
    assert metrics["scanned_rows"].value == 1_000_000
    assert metrics["output_rows"].value == 10


def test_data_size_is_not_scan_bytes():
    metrics = canonical_metrics([accum("data size", 4096, "size")])
    assert "scan_bytes" not in metrics
    assert metrics["broadcast_payload_bytes"].value == 4096


def test_operator_spill_is_its_own_metric():
    photon = canonical_metrics([accum("numBytesSpilled", 1024, "size")])
    classic = canonical_metrics([accum("spill size", 1024, "size")])
    assert photon["spill_bytes"].value == classic["spill_bytes"].value == 1024
    assert "memory_bytes_spilled" not in photon
    assert "disk_bytes_spilled" not in photon


def test_cumulative_time_keeps_cumulative_aggregation():
    definition = definition_for("cumulTime")
    assert definition is not None
    assert definition.aggregation == MetricAggregation.cumulative


# ---------------------------------------------------------------------------
# normalization
# ---------------------------------------------------------------------------


def test_unknown_metric_is_preserved_without_a_canonical_name():
    metric = normalize_metric(accum("photonInternalThing", 7, "size"))
    assert metric is not None
    assert metric.canonical is None
    assert metric.raw_name == "photonInternalThing"
    assert metric.value == 7
    assert metric.unit == MetricUnit.bytes
    assert metric.derivation == MetricDerivation.inferred_from_metric_type


def test_unknown_metric_without_a_type_has_no_unit():
    metric = normalize_metric(accum("mysteryMetric", 3, "somethingElse"))
    assert metric is not None
    assert metric.unit == MetricUnit.none


def test_zero_is_preserved_as_zero():
    assert metric_value([accum("number of output rows", 0)], "output_rows") == 0


def test_missing_metric_is_none_not_zero():
    assert metric_value([accum("number of files read", 1)], "output_rows") is None
    assert metric_value(None, "output_rows") is None
    assert metric_value([], "output_rows") is None


def test_null_valued_metric_reports_unavailable_coverage():
    metric = normalize_metric(accum("number of output rows", None))
    assert metric is not None
    assert metric.value is None
    assert metric.coverage == CapabilityStatus.unavailable


def test_row_counts_stay_exact_integers():
    value = metric_value(
        [accum("number of output rows", 123_456_789_012)], "output_rows"
    )
    assert isinstance(value, int)
    assert value == 123_456_789_012


def _raw(name: str, value, metric_type: str = "sum") -> dict:
    """An accumulator struct whose value bypasses the float64 frame column."""
    raw = accum(name, 0, metric_type)
    raw["value"] = value
    return raw


def test_integer_counts_above_2_53_are_not_rounded():
    huge = 2**53 + 1
    assert metric_value([_raw("number of output rows", huge)], "output_rows") == huge


def test_integral_strings_are_parsed_without_a_float_detour():
    huge = 2**53 + 1
    value = metric_value([_raw("numOutputRows", str(huge))], "output_rows")
    assert isinstance(value, int)
    assert value == huge


def test_large_counts_round_trip_through_json():
    huge = 2**53 + 1
    metric = normalize_metric(_raw("number of output rows", huge))
    assert metric is not None
    restored = json.loads(json.dumps(metric.model_dump(mode="json")))
    assert restored["value"] == huge
    assert isinstance(restored["value"], int)


def test_fractional_values_are_not_narrowed_to_int():
    metric = normalize_metric(_raw("number of output rows", 10.5))
    assert metric is not None
    assert metric.value == 10.5


def test_timing_values_keep_precision():
    metric = normalize_metric(accum("scan time", 1772.5, "timing"))
    assert metric is not None
    assert metric.value == 1772.5
    assert metric.unit == MetricUnit.milliseconds


def test_metrics_round_trip_through_json_without_loss():
    metrics = normalize_metrics(
        [
            accum("number of output rows", 40960),
            accum("size of files read", 255533748, "size"),
            accum("scan time", 1772.5, "timing"),
        ]
    )
    payload = json.dumps([metric.model_dump(mode="json") for metric in metrics])
    restored = json.loads(payload)
    assert [entry["value"] for entry in restored] == [40960, 255533748, 1772.5]
    assert isinstance(restored[0]["value"], int)


def test_first_alias_with_a_value_wins():
    metrics = canonical_metrics(
        [accum("number of output rows", 5), accum("numOutputRows", 9)]
    )
    assert metrics["output_rows"].value == 5


def test_a_null_alias_does_not_mask_a_measured_one():
    metrics = canonical_metrics(
        [accum("number of output rows", None), accum("numOutputRows", 9)]
    )
    assert metrics["output_rows"].value == 9


def test_malformed_entries_are_skipped():
    assert normalize_metric("not a metric") is None
    assert normalize_metric({"value": 1}) is None
    assert normalize_metrics([{"value": 1}, accum("duration", 5, "timing")]) != []


def test_source_label_is_carried_through():
    metric = normalize_metric(accum("numOutputRows", 1), SOURCE_CONNECT_PLAN)
    assert metric is not None
    assert metric.source == SOURCE_CONNECT_PLAN
