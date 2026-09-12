"""Offline regression tests for the Spark Connect adapter.

No Spark, gRPC, or protobuf runtime is required: the fakes below reproduce the
duck-typed surface the adapter actually touches (``PlanMetrics``-shaped metrics and
``Relation``-shaped plan protos).
"""

from __future__ import annotations

import dataclasses
import json
import threading
from pathlib import Path
from typing import Any, cast

import pytest

from sparkparse.capture import SparkparseCapture, _capabilities
from sparkparse.connect import (
    SparkConnectCapture,
    probe_connect_support,
    resolve_node_type,
)
from sparkparse.models import CapabilityStatus, NodeType

FIXTURE_DIR = Path(__file__).parent / "data" / "connect"


# --------------------------------------------------------------------- fake metrics


@dataclasses.dataclass(frozen=True)
class FakeMetric:
    name: str
    value: float
    metric_type: str


@dataclasses.dataclass(frozen=True)
class FakePlanMetrics:
    name: str
    plan_id: int
    parent_plan_id: int
    metrics: tuple[FakeMetric, ...] = ()


def node(
    name: str, plan_id: int, parent_plan_id: int, **metrics: tuple[float, str]
) -> FakePlanMetrics:
    return FakePlanMetrics(
        name=name,
        plan_id=plan_id,
        parent_plan_id=parent_plan_id,
        metrics=tuple(
            FakeMetric(metric_name, value, metric_type)
            for metric_name, (value, metric_type) in metrics.items()
        ),
    )


# ----------------------------------------------------------------------- fake protos


class FakeCommon:
    def __init__(self, plan_id: int | None) -> None:
        self.plan_id = plan_id

    def HasField(self, name: str) -> bool:
        return name == "plan_id" and self.plan_id is not None


class FakeRelation:
    def __init__(self, rel_type: str, inner: Any, plan_id: int | None = None) -> None:
        self._rel_type = rel_type
        setattr(self, rel_type, inner)
        self.common = FakeCommon(plan_id)

    def WhichOneof(self, oneof: str) -> str | None:
        return self._rel_type if oneof == "rel_type" else None


class FakeInner:
    def __init__(self, **fields: Any) -> None:
        self._fields = fields
        for key, value in fields.items():
            setattr(self, key, value)

    def ListFields(self) -> list[tuple[str, Any]]:
        return list(self._fields.items())


class FakeJoin(FakeInner):
    def __init__(
        self,
        left: FakeRelation,
        right: FakeRelation,
        join_type: int = 1,
        using_columns: tuple[str, ...] = (),
        join_condition: Any = None,
    ) -> None:
        super().__init__(left=left, right=right)
        self.join_type = join_type
        self.using_columns = list(using_columns)
        self.join_condition = join_condition


class FakeExpression:
    def __init__(self, expr_type: str, inner: Any) -> None:
        self._expr_type = expr_type
        setattr(self, expr_type, inner)

    def WhichOneof(self, oneof: str) -> str | None:
        return self._expr_type if oneof == "expr_type" else None


def attribute(name: str) -> FakeExpression:
    return FakeExpression("unresolved_attribute", FakeInner(unparsed_identifier=name))


def equals(left: str, right: str) -> FakeExpression:
    return FakeExpression(
        "unresolved_function",
        FakeInner(function_name="=", arguments=[attribute(left), attribute(right)]),
    )


def scan_relation(table: str, plan_id: int | None = None) -> FakeRelation:
    return FakeRelation("read", FakeInner(named_table=table), plan_id=plan_id)


def join_plan(
    plan_id: int | None = None,
    using_columns: tuple[str, ...] = (),
    join_condition: Any = None,
    join_type: int = 1,
) -> FakePlan:
    relation = FakeRelation(
        "join",
        FakeJoin(
            left=scan_relation("left_table"),
            right=scan_relation("right_table"),
            join_type=join_type,
            using_columns=using_columns,
            join_condition=join_condition,
        ),
        plan_id=plan_id,
    )
    return FakePlan(relation)


class FakePlan:
    def __init__(self, root: FakeRelation | None) -> None:
        self.root = root

    def HasField(self, name: str) -> bool:
        return name == "root" and self.root is not None


# ----------------------------------------------------------------------- fake client


@dataclasses.dataclass(frozen=True)
class FakeRequest:
    # Matches the real client: the field is populated only when a caller passes an id
    # into the builder, which the action paths never do.
    operation_id: str = ""


@dataclasses.dataclass(frozen=True)
class FakeResponse:
    operation_id: str


class LegacyFakeClient:
    """A client stub predating ``_verify_response_integrity``, with no response hook."""

    def __init__(self, session_id: str = "session-1") -> None:
        self.session_id = session_id
        self.queued: dict[str, list[list[Any]]] = {}
        self.operation_ids: list[str] = []
        self.received: list[list[Any]] = []

    def queue(self, action: str, *batches: list[Any]) -> None:
        self.queued.setdefault(action, []).append(list(batches))

    def _next(self, action: str) -> list[list[Any]]:
        pending = self.queued.get(action) or []
        return pending.pop(0) if pending else []

    def _build_metrics(self, metrics: list[Any]) -> Any:
        return (metric for metric in metrics)

    def _execute_plan_request_with_metadata(self, operation_id: str | None = None):
        return FakeRequest(operation_id or "")

    def _emit(self, action: str) -> list[Any]:
        self._execute_plan_request_with_metadata()
        response = FakeResponse(f"op-{len(self.operation_ids)}")
        self.operation_ids.append(response.operation_id)
        verify = getattr(self, "_verify_response_integrity", None)
        if callable(verify):
            verify(response)
        collected: list[Any] = []
        for batch in self._next(action):
            collected.extend(self._build_metrics(batch))
        self.received.append(collected)
        return collected

    def to_table(self, plan: Any, observations: Any = None) -> tuple[Any, Any, Any]:
        return "table", "schema", self._emit("to_table")

    def to_pandas(self, plan: Any, observations: Any = None) -> tuple[Any, Any]:
        return "pandas", self._emit("to_pandas")

    def to_table_as_iterator(self, plan: Any, observations: Any = None):
        self._emit("to_table_as_iterator")
        yield "batch-1"
        yield "batch-2"

    def execute_command(self, command: Any, observations: Any = None):
        return None, {}, self._emit("execute_command")

    def execute_command_as_iterator(self, command: Any, observations: Any = None):
        self._emit("execute_command_as_iterator")
        yield {}


class FakeClient(LegacyFakeClient):
    """The current client shape: metrics through ``_build_metrics``, plus the response
    hook that carries the server-assigned operation id."""

    def _verify_response_integrity(self, response: Any) -> None:
        return None


class FakeSpark:
    def __init__(self, client: Any) -> None:
        self._client = client


def capture_for(client: Any, **kwargs: Any) -> SparkConnectCapture:
    return SparkConnectCapture(spark=cast(Any, FakeSpark(client)), **kwargs)


def details_for(cap: SparkConnectCapture, node_id: int, query_id: int = 0) -> dict:
    assert cap.dfs is not None
    row = cap.dfs.dag.filter(
        (cap.dfs.dag["node_id"] == node_id) & (cap.dfs.dag["query_id"] == query_id)
    ).to_dicts()[0]
    return json.loads(row["details"])["detail"]


def codes(cap: SparkConnectCapture) -> set[str]:
    return {diagnostic.code for diagnostic in cap.diagnostics}


# ----------------------------------------------------- increment 1: mapping and hooks


@pytest.mark.parametrize(
    ("raw", "expected", "source"),
    [
        ("PhotonGroupingAgg (12)", NodeType.HashAggregate, "alias"),
        ("Project [a, b]", NodeType.Project, "canonical"),
        ("PhotonShuffleMapStage", NodeType.ShuffleQueryStage, "alias"),
        ("PhotonSomethingNew", NodeType.Unknown, "unknown"),
        ("", NodeType.Unknown, "unknown"),
    ],
)
def test_node_types_map_or_degrade_to_unknown(raw, expected, source):
    assert resolve_node_type(raw) == (expected, source)


def test_strict_mode_rejects_unknown_operators():
    with pytest.raises(ValueError, match="Unknown Spark Connect operator"):
        resolve_node_type("PhotonSomethingNew", strict=True)


def test_unknown_operator_is_preserved_with_edges_and_raw_name():
    client = FakeClient()
    client.queue(
        "to_table",
        [
            node("PhotonResultStage", 1, 1),
            node("PhotonBrandNewOp attr", 2, 1),
            node("PhotonScan parquet main.db.tbl [a]", 3, 2),
        ],
    )
    cap = capture_for(client)
    with cap:
        client.to_table(FakePlan(None))

    assert cap.dfs is not None
    dag = cap.dfs.dag
    unknown = dag.filter(dag["node_id"] == 2).to_dicts()[0]
    assert unknown["node_type"] == NodeType.Unknown
    assert "PhotonBrandNewOp" in unknown["node_name"]
    assert unknown["child_nodes"] == "3"
    assert details_for(cap, 2)["raw"] == "PhotonBrandNewOp attr"
    assert "unknown_operators" in codes(cap)
    # The unknown node is not relabeled as an exchange to keep the graph drawable.
    assert dag.filter(dag["node_type"] == NodeType.Exchange).is_empty()


def test_strict_capture_raises_on_unknown_operator():
    client = FakeClient()
    client.queue("to_table", [node("PhotonBrandNewOp", 1, 1)])
    cap = capture_for(client, strict=True)
    with pytest.raises(ValueError, match="Unknown Spark Connect operator"):
        with cap:
            client.to_table(FakePlan(None))
    assert client._build_metrics.__self__ is client  # hooks were restored


def test_build_metrics_hook_returns_a_consumable_iterator():
    client = FakeClient()
    batch = [node("Project", 1, 1)]
    client.queue("to_table", batch)
    cap = capture_for(client)
    with cap:
        _, _, collected = client.to_table(FakePlan(None))

    assert [item.plan_id for item in collected] == [1]
    assert cap.dfs is not None
    assert cap.dfs.dag.height == 1


def test_empty_metrics_produce_no_rows_but_report_missing_coverage():
    client = FakeClient()
    cap = capture_for(client)
    with cap:
        client.to_table(FakePlan(None))

    assert cap.dfs is not None
    assert cap.dfs.dag.is_empty()
    assert "no_operator_metrics" in codes(cap)
    assert cap.executions[0]["action"] == "to_table"


def test_malformed_metrics_are_reported_and_skipped():
    class Broken:
        plan_id = "not-an-int"
        parent_plan_id = 1
        name = "Project"
        metrics: tuple = ()

    client = FakeClient()
    client.queue(
        "to_table",
        [Broken(), node("Project", 2, 2, numOutputRows=(10, "sum"))],
    )
    cap = capture_for(client)
    with cap:
        client.to_table(FakePlan(None))

    assert cap.dfs is not None
    assert cap.dfs.dag["node_id"].to_list() == [2]
    assert "malformed_metrics" in codes(cap)


def test_missing_metrics_method_fails_before_the_workload_runs():
    class NoMetrics:
        def to_table(self, plan, observations=None):
            raise AssertionError("the workload must not run")

    with pytest.raises(ValueError, match="_build_metrics"):
        with capture_for(NoMetrics()):
            pass


def test_missing_action_methods_fail_and_partial_support_is_reported():
    class OnlyMetrics:
        def _build_metrics(self, metrics):
            return iter(metrics)

    with pytest.raises(ValueError, match="no interceptable action methods"):
        with capture_for(OnlyMetrics()):
            pass

    class TableOnly(OnlyMetrics):
        def to_table(self, plan, observations=None):
            return "table", "schema", []

    cap = capture_for(TableOnly())
    with cap:
        pass
    assert "action_not_covered" in codes(cap)
    assert cap.support is not None
    assert "to_pandas" in cap.support.missing_actions


def test_unexpected_metrics_signature_is_reported_but_still_captured():
    class Variadic(FakeClient):
        def _build_metrics(self, metrics, *extra):
            return (metric for metric in metrics)

    client = Variadic()
    client.queue("to_table", [node("Project", 1, 1)])
    cap = capture_for(client)
    with cap:
        client.to_table(FakePlan(None))

    assert cap.support is not None
    assert cap.support.metrics_signature_verified is True
    assert cap.dfs is not None and cap.dfs.dag.height == 1

    class Renamed:
        def _build_metrics(self, metrics, required_extra):
            return (metric for metric in metrics)

        def to_table(self, plan, observations=None):
            return "table", "schema", []

    reporting = capture_for(Renamed())
    with reporting:
        pass
    assert reporting.support is not None
    assert reporting.support.metrics_signature_verified is False
    assert "unverified_client_api" in codes(reporting)


def test_setup_failure_after_the_first_hook_restores_every_original():
    class Fragile(FakeClient):
        def __setattr__(self, name: str, value: Any) -> None:
            if name == "to_pandas":
                raise RuntimeError("cannot patch")
            super().__setattr__(name, value)

    client = Fragile()
    original = client.to_table
    with pytest.raises(RuntimeError, match="cannot patch"):
        with capture_for(client):
            pass

    assert client.to_table == original
    assert "_build_metrics" not in client.__dict__
    assert "to_table" not in client.__dict__


def test_hooks_are_restored_exactly_and_leave_no_instance_attributes():
    client = FakeClient()
    with capture_for(client):
        assert "_build_metrics" in client.__dict__
    assert client.__dict__.keys() == {
        "session_id",
        "queued",
        "operation_ids",
        "received",
    }


def test_capture_on_an_already_captured_client_is_rejected():
    client = FakeClient()
    with capture_for(client):
        with pytest.raises(RuntimeError, match="already being captured"):
            with capture_for(client):
                pass


def test_repeated_contexts_reset_per_entry_state():
    client = FakeClient()
    client.queue("to_table", [node("Project", 1, 1)])
    client.queue("to_table", [node("Filter", 5, 5)])
    cap = capture_for(client)

    with cap:
        client.to_table(FakePlan(None))
    first = cap.dfs
    with cap:
        client.to_table(FakePlan(None))
    second = cap.dfs

    assert first is not None and second is not None
    assert first.dag["node_id"].to_list() == [1]
    assert second.dag["node_id"].to_list() == [5]
    assert second.dag["query_id"].to_list() == [0]
    assert len(cap.executions) == 1


def test_user_exception_retains_partial_execution_data():
    client = FakeClient()
    client.queue("to_table", [node("Project", 1, 1)])
    cap = capture_for(client)

    with pytest.raises(ValueError, match="workload failed"):
        with cap:
            client.to_table(FakePlan(None))
            raise ValueError("workload failed")

    assert cap.dfs is not None
    assert cap.dfs.dag.height == 1
    assert "_build_metrics" not in client.__dict__


# ------------------------------------------ increment 2: correlation and topology


def test_joins_do_not_leak_keys_between_queries():
    client = FakeClient()
    client.queue("to_table", [node("PhotonSortMergeJoin", 7, 7)])
    client.queue("to_table", [node("PhotonSortMergeJoin", 9, 9)])
    cap = capture_for(client)

    with cap:
        client.to_table(join_plan(plan_id=7, using_columns=("customer_id",)))
        client.to_table(join_plan(plan_id=9, using_columns=("order_id",)))

    assert details_for(cap, 7, query_id=0)["left_keys"] == ["customer_id"]
    assert details_for(cap, 9, query_id=1)["left_keys"] == ["order_id"]


def test_join_keys_are_matched_by_plan_id_not_position():
    client = FakeClient()
    client.queue(
        "to_table",
        [
            node("PhotonSortMergeJoin", 11, 11),
            node("PhotonBroadcastHashJoin", 12, 11),
        ],
    )
    outer = FakeRelation(
        "join",
        FakeJoin(
            left=FakeRelation(
                "join",
                FakeJoin(
                    left=scan_relation("c"),
                    right=scan_relation("d"),
                    using_columns=("inner_key",),
                ),
                plan_id=12,
            ),
            right=scan_relation("e"),
            using_columns=("outer_key",),
        ),
        plan_id=11,
    )
    cap = capture_for(client)
    with cap:
        client.to_table(FakePlan(outer))

    assert details_for(cap, 11)["left_keys"] == ["outer_key"]
    assert details_for(cap, 11)["join_details_source"] == "logical_plan_id_match"
    assert details_for(cap, 12)["left_keys"] == ["inner_key"]


def test_unmatchable_joins_stay_unresolved_instead_of_positional():
    client = FakeClient()
    client.queue(
        "to_table",
        [
            node("PhotonSortMergeJoin", 21, 21),
            node("PhotonBroadcastHashJoin", 22, 21),
        ],
    )
    outer = FakeRelation(
        "join",
        FakeJoin(
            left=FakeRelation(
                "join",
                FakeJoin(
                    left=scan_relation("c"),
                    right=scan_relation("d"),
                    using_columns=("inner_key",),
                ),
            ),
            right=scan_relation("e"),
            using_columns=("outer_key",),
        ),
    )
    cap = capture_for(client)
    with cap:
        client.to_table(FakePlan(outer))

    for node_id in (21, 22):
        detail = details_for(cap, node_id)
        assert "left_keys" not in detail
        assert detail["join_keys_resolved"] is False
        assert detail["input_roles"] == "unordered"
    assert "join_details_unresolved" in codes(cap)


def test_aqe_reordered_physical_joins_do_not_borrow_logical_keys():
    """Physical plan IDs that AQE never produced must not inherit logical join keys."""
    client = FakeClient()
    client.queue(
        "to_table",
        [
            node("PhotonSortMergeJoin", 21, 21),
            node("PhotonBroadcastHashJoin", 99, 21),
        ],
    )
    outer = FakeRelation(
        "join",
        FakeJoin(
            left=FakeRelation(
                "join",
                FakeJoin(
                    left=scan_relation("c"),
                    right=scan_relation("d"),
                    using_columns=("inner_key",),
                ),
                plan_id=22,
            ),
            right=scan_relation("e"),
            using_columns=("outer_key",),
        ),
        plan_id=21,
    )
    cap = capture_for(client)
    with cap:
        client.to_table(FakePlan(outer))

    assert details_for(cap, 21)["left_keys"] == ["outer_key"]
    unmatched = details_for(cap, 99)
    assert "left_keys" not in unmatched
    assert unmatched["join_details_source"] == "unresolved"
    assert "join_details_unresolved" in codes(cap)


def test_single_join_without_plan_ids_is_still_resolved():
    client = FakeClient()
    client.queue("to_table", [node("PhotonSortMergeJoin", 31, 31)])
    cap = capture_for(client)
    with cap:
        client.to_table(join_plan(using_columns=("id",), join_type=7))

    detail = details_for(cap, 31)
    assert detail["join_details_source"] == "single_join_in_query"
    assert detail["join_type"] == "Cross"


def test_condition_joins_report_columns_without_asserting_sides():
    client = FakeClient()
    client.queue("to_table", [node("PhotonBroadcastHashJoin", 41, 41)])
    cap = capture_for(client)
    with cap:
        client.to_table(join_plan(plan_id=41, join_condition=equals("l_id", "r_id")))

    detail = details_for(cap, 41)
    assert detail["join_condition_columns"] == ["l_id", "r_id"]
    assert detail["join_keys_resolved"] is False
    assert "left_keys" not in detail


def test_oss_node_names_supply_join_keys_when_the_plan_cannot():
    client = FakeClient()
    client.queue(
        "to_table",
        [node("SortMergeJoin [a#1], [b#2], Inner, BuildRight", 51, 51)],
    )
    cap = capture_for(client)
    with cap:
        client.to_table(FakePlan(None))

    detail = details_for(cap, 51)
    assert detail["left_keys"] == ["a"]
    assert detail["right_keys"] == ["b"]
    assert detail["join_type"] == "Inner"
    assert detail["join_details_source"] == "node_name"


def test_repeated_snapshots_are_not_summed():
    client = FakeClient()
    first = [node("PhotonScan parquet main.db.t [a]", 61, 61, numOutputRows=(5, "sum"))]
    final = [
        node("PhotonScan parquet main.db.t [a]", 61, 61, numOutputRows=(42, "sum"))
    ]
    client.queue("to_table", first, final)
    cap = capture_for(client)
    with cap:
        client.to_table(FakePlan(None))

    assert cap.dfs is not None
    row = cap.dfs.dag.to_dicts()[0]
    assert [metric["value"] for metric in row["accumulator_totals"]] == [42.0]
    assert cap.executions[0]["metric_batches"] == 2


def test_repeated_actions_on_one_dataframe_are_separate_queries():
    client = FakeClient()
    batch = [node("PhotonScan parquet main.db.t [a]", 71, 71, numOutputRows=(3, "sum"))]
    client.queue("to_table", list(batch))
    client.queue("to_table", list(batch))
    plan = join_plan(plan_id=71)
    cap = capture_for(client)
    with cap:
        client.to_table(plan)
        client.to_table(plan)

    assert cap.dfs is not None
    assert cap.dfs.dag["query_id"].to_list() == [0, 1]
    assert cap.dfs.dag["source_execution_id"].to_list() == ["op-0", "op-1"]


def test_interleaved_threads_attribute_metrics_by_calling_thread():
    client = FakeClient()
    client.queue("to_table", [node("Project", 81, 81)])
    client.queue("to_pandas", [node("Filter", 91, 91)])
    cap = capture_for(client)
    started = threading.Barrier(2)

    def run_pandas():
        started.wait(timeout=5)
        client.to_pandas(FakePlan(None))

    with cap:
        worker = threading.Thread(target=run_pandas)
        worker.start()
        started.wait(timeout=5)
        client.to_table(FakePlan(None))
        worker.join(timeout=5)

    assert cap.dfs is not None
    by_query = {row["query_id"]: row["node_id"] for row in cap.dfs.dag.to_dicts()}
    actions = {
        execution["query_id"]: execution["action"] for execution in cap.executions
    }
    for query_id, node_id in by_query.items():
        assert (actions[query_id], node_id) in {("to_table", 81), ("to_pandas", 91)}


def test_metrics_outside_an_action_boundary_are_marked_unattributed():
    client = FakeClient()
    cap = capture_for(client)
    with cap:
        list(client._build_metrics([node("Project", 101, 101)]))

    assert cap.dfs is not None
    row = cap.dfs.dag.to_dicts()[0]
    assert row["query_start_timestamp"] is None
    assert "unattributed_metrics" in codes(cap)
    assert cap.executions[0]["attributed"] is False


# ------------------------------------ increment 3: timing semantics and capabilities


@pytest.mark.parametrize(
    ("action", "call"),
    [
        ("to_table", lambda client: client.to_table(FakePlan(None))),
        ("to_pandas", lambda client: client.to_pandas(FakePlan(None))),
        (
            "to_table_as_iterator",
            lambda client: list(client.to_table_as_iterator(FakePlan(None))),
        ),
        ("execute_command", lambda client: client.execute_command(object())),
        (
            "execute_command_as_iterator",
            lambda client: list(client.execute_command_as_iterator(object())),
        ),
    ],
)
def test_each_action_kind_is_covered_as_its_own_query(action, call):
    client = FakeClient()
    client.queue(action, [node("Project", 1, 1, cumulTime=(6e10, "nsTiming"))])
    cap = capture_for(client)
    with cap:
        call(client)

    assert cap.dfs is not None
    row = cap.dfs.dag.to_dicts()[0]
    assert row["query_function"] == action
    assert row["query_start_timestamp"] is not None
    assert row["query_end_timestamp"] is not None
    assert row["query_duration_seconds"] is not None
    assert row["node_duration_minutes"] == pytest.approx(1.0)


def test_client_elapsed_time_is_not_taken_from_cumulative_operator_time():
    client = FakeClient()
    client.queue(
        "to_table",
        [node("PhotonResultStage", 1, 1, cumulTime=(7_200e9, "nsTiming"))],
    )
    cap = capture_for(client)
    with cap:
        client.to_table(FakePlan(None))

    assert cap.dfs is not None
    row = cap.dfs.dag.to_dicts()[0]
    assert row["query_duration_seconds"] < 60
    cumulative = details_for(cap, 1)["cumulative_operator_time"]
    assert cumulative["unit"] == "ns"
    assert cumulative["value"] == 7_200e9
    assert "not wall-clock" in cumulative["semantics"]


def test_connect_elapsed_coverage_is_partial_and_task_coverage_unavailable():
    client = FakeClient()
    client.queue("to_table", [node("Project", 1, 1, numOutputRows=(2, "sum"))])
    cap = capture_for(client)
    with cap:
        client.to_table(FakePlan(None))

    assert cap.dfs is not None
    capabilities = _capabilities(cap.dfs, "spark_connect")
    assert capabilities.query_elapsed_time.status == CapabilityStatus.partial
    assert "transfer" in (capabilities.query_elapsed_time.reason or "")
    assert capabilities.task_metrics.status == CapabilityStatus.unavailable
    assert capabilities.plan_structure.status == CapabilityStatus.available


def test_capture_result_carries_connect_diagnostics_and_execution_ids():
    client = FakeClient()
    client.queue("to_table", [node("PhotonBrandNewOp", 1, 1)])
    outer = SparkparseCapture(
        "get", spark=cast(Any, FakeSpark(client)), backend="connect"
    )
    with outer:
        client.to_table(FakePlan(None))

    assert outer.result is not None
    assert "unknown_operators" in {d.code for d in outer.result.diagnostics}
    assert outer.result.dag["source_execution_id"].to_list() == ["op-0"]
    assert outer.result.metadata.backend == "spark_connect"


def test_probe_reports_client_surface_without_a_live_connection():
    support = probe_connect_support(FakeSpark(FakeClient()))

    assert support.metrics_hook is True
    assert support.metrics_signature_verified is True
    assert "to_table" in support.hookable_actions
    assert support.missing_actions == ()
    assert support.client_version is not None
    assert isinstance(support.as_dict(), dict)


def test_probe_reports_unusable_clients():
    support = probe_connect_support(object())

    assert support.metrics_hook is False
    assert set(support.missing_actions) >= {"to_table", "execute_command"}
    assert any("_build_metrics" in note for note in support.notes)


# ------------------------------------------------------------------ fixture replay


def test_recorded_executions_round_trip_through_replay():
    client = FakeClient()
    client.queue(
        "to_table",
        [node("PhotonScan parquet main.db.t [a]", 1, 1, numOutputRows=(7, "sum"))],
    )
    cap = capture_for(client, log_name="live")
    with cap:
        client.to_table(FakePlan(None))

    replayed = SparkConnectCapture.from_plan_metrics(
        cap.to_plan_metrics(), log_name="live"
    )

    assert cap.dfs is not None and replayed.dfs is not None
    compared = ["query_id", "node_id", "node_type", "accumulator_totals", "details"]
    assert replayed.dfs.dag.select(compared).equals(cap.dfs.dag.select(compared))
    assert replayed.dfs.dag["source_execution_id"].to_list() == ["op-0"]


def test_sanitized_fixture_replays_offline():
    """Replay of a real Databricks serverless recording (runtime 4.2.0, client 3.5.0).

    Two joined ``spark.range`` frames, recorded by
    ``notebooks/validate_connect_capture.py``; operator names are sanitized. The run
    produced two structurally identical executions and only one is kept: they share a
    node sequence and metric-name set, so the second exercises no parse path the first
    does not. Per-query isolation is covered against the fake client instead.
    """
    fixture = json.loads((FIXTURE_DIR / "photon_join_execution.json").read_text())
    assert fixture["source"] == "databricks_serverless"
    cap = SparkConnectCapture.from_plan_metrics(
        fixture["executions"], log_name="fixture"
    )

    assert cap.dfs is not None
    dag = cap.dfs.dag
    assert len(dag) == 20
    assert dag["query_duration_seconds"].unique().to_list() == [0.568]

    # A server-assigned operation id, carried through to the DAG.
    (execution_id,) = dag["source_execution_id"].unique().to_list()
    assert len(execution_id) == 36

    node_types = {str(node_type) for node_type in dag["node_type"].to_list()}
    assert "BroadcastHashJoin" in node_types
    assert "ShuffleQueryStage" in node_types

    # Photon emits operators the mapping does not know; they are preserved as Unknown
    # nodes rather than dropped, and reported once.
    assert "Unknown" in node_types
    assert [d.code for d in cap.diagnostics] == ["unknown_operators"]

    # Databricks does not propagate the client-assigned plan id into physical Photon
    # nodes, so join keys stay unresolved rather than being matched by position.
    assert details_for(cap, 11726)["join_details_source"] == "unresolved"


def test_operation_id_comes_from_the_response_not_the_request():
    """Regression: live serverless recorded no operation ids at all.

    ``ExecutePlanRequest.operation_id`` is an unset optional string unless the caller
    supplies one, so reading it off the request left every execution with ``None``. The
    server-assigned id arrives on each ``ExecutePlanResponse``.
    """
    client = FakeClient()
    client.queue("to_table", [node("Project", 1, 1)])
    client.queue("execute_command", [])
    with capture_for(client) as capture:
        client.to_table(FakePlan(None))
        client.execute_command(object())

    recorded = [execution["operation_id"] for execution in capture.executions]
    assert recorded == ["op-0", "op-1"]


def test_command_executions_record_an_operation_id_without_operator_metrics():
    client = FakeClient()
    client.queue("execute_command", [])
    with capture_for(client) as capture:
        client.execute_command(object())

    (execution,) = capture.executions
    assert execution["operation_id"] == "op-0"
    assert execution["n_nodes"] == 0


def test_a_caller_supplied_request_operation_id_is_still_honoured():
    client = FakeClient()
    client.queue("to_table", [node("Project", 1, 1)])
    with capture_for(client) as capture:
        client._execute_plan_request_with_metadata("caller-supplied")
        client.to_table(FakePlan(None))

    (execution,) = capture.executions
    assert execution["operation_id"] == "op-0"


def test_a_client_without_the_response_hook_still_captures():
    """Older clients expose no ``_verify_response_integrity``; capture degrades, not fails."""
    client = LegacyFakeClient()
    client.queue("to_table", [node("Project", 1, 1)])
    with capture_for(client) as capture:
        client.to_table(FakePlan(None))

    (execution,) = capture.executions
    assert execution["operation_id"] is None
    assert execution["n_nodes"] == 1
